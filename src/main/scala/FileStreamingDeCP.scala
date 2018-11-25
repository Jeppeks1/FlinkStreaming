import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.io.SerializedOutputFormat
import org.apache.flink.api.common.io.SerializedInputFormat
import org.apache.flink.api.common.operators.Order

import org.slf4j.{Logger, LoggerFactory}

import container.InternalNode._
import container.Point
import functions._
import reader._


/**
  * The StreamingDeCP object implements the streaming version of the DeCP method. The
  * limitations of Flink regarding connecting a DataSet and DataStream leads to two
  * different workarounds being utilized:
  * <li>The entire set of clustered points is kept in a static variable for easy
  * access by all downstream operators in the streaming pipeline.</li>
  * <li>The clustered points are written to a unique file depending on the clusterID,
  * which allows the downstream processing operators to read only the intended clusters.
  * At least one IO is required for every query point however.</li>
  *
  * This file contains the second workaround.
  */
object FileStreamingDeCP {

  val log: Logger = LoggerFactory.getLogger("FileStreamingDeCP")
  val recordSize: Int = 128 * 2 + 8 // 128 chars of two bytes each, eight bytes from the Long pointID

  /**
    * Usage:
    * {{{
    *   StreamingDeCP --sift <String>
    *                 --method <String>
    *                 --recluster <boolean>
    *                 --clusterSize <Int>
    *                 --treeA <Int>
    *                 --L <Int>
    *                 --a <Int>
    *                 --b <Int>
    *                 --k <Int>
    * }}}
    */
  def main(args: Array[String]): Unit = {
    // Get the input parameters
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // Get and initialize variables from the parameter tool
    val recluster = params.getRequired("recluster").toBoolean
    val method = params.getRequired("method")
    val sift = params.getRequired("sift")
    val L = params.get("L", "2").toInt
    val a = params.get("a", "1").toInt
    val b = params.get("b", "5").toInt
    val k = params.get("k", "100").toInt
    val treeA = params.get("treeA", "3").toInt

    // Set the paths and configuration properties
    val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/" + sift
    // val siftPath = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftmedium\\"
    val featureVectorPath = new Path(siftPath + "/base.fvecs")
    val groundTruthPath = new Path(siftPath + "/groundtruth.ivecs")
    val queryPointPath = new Path(siftPath + "/query.fvecs")
    val clusterPath = new Path(siftPath + "/cluster/")
    val outputPath = new Path(siftPath + "/output.csv")
    val indexPath = new Path(siftPath + "/cluster/indexLeafs")

    // Get the ExecutionEnvironments and read the data using a PointInputFormat
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val queryPoints: DataStream[Point] = streamEnv.readFile(new PointInputFormat(queryPointPath), queryPointPath.toString)
    val points: DataSet[Point] = env.createInput(new PointInputFormat(featureVectorPath)).name("Point Source")
    val truth: DataSet[(Int, Array[Int])] = env.createInput(new TruthInputFormat(groundTruthPath)).setParallelism(4)

    // Collect the ground truth so it can be accessed by the streaming method
    val groundTruth = truth.name("Truth Source").collect.toArray

    // Perform the kNN scan or search
    val knn = if (method == "scan") {

      if (recluster){
        // Set the number of clusters to write the points to
        val clusterSize = params.getRequired("clusterSize").toInt

        // Assert the target path is empty before reclustering
        checkTargetPath(clusterPath)

        // Measure the time it takes to complete the scan
        val clusterStart = System.currentTimeMillis

        // Distribute the points randomly to clusterSize clusters
        points.map(p => (p, math.ceil(math.random * clusterSize).toLong)).name("RandomCluster")
          .sortPartition(_._2, Order.ASCENDING).setParallelism(1).name("SortPartition")
          .write(new ClusterOutputFormat(clusterPath), clusterPath.toString, WriteMode.NO_OVERWRITE).name("SinkToCluster")
          .setParallelism(1)

        // Force the execution of the clustering, so that the throughput metric is accurate
        env.execute("FileStreaming DeCP - Clustering")

        // Write to the log
        log.info("Clustering the points finished in " + (System.currentTimeMillis - clusterStart) + " milliseconds")
      }

      // Rebalance the incoming Points and perform a sequential scan by reading one clusterID at a time
      val knn = queryPoints.name("QueryPoints Source").rebalance
        .map(new FileStreamingSequentialScan(clusterPath, k)).name("FileStreamingSequentialScan")
        .map(new StreamingGroundTruth(groundTruth, k)).name("StreamingGroundTruth")

      // Forcing the streamEnv here and again at the writeAsCsv method is not an option,
      // as the above code will be executed twice. The writeAsCsv method is fast enough,
      // that I include the time it takes to write the output in the metric that measures
      // the search or scan time.

      knn
    }
    else if (method == "index") {

      val leafs = if (recluster){
        // Assert the target path is empty before reclustering
        checkTargetPath(clusterPath)

        // Build the root node of the index for the index search and get the leafs
        val root = buildTheIndex(points, recordSize, treeA, L)
        val leafs = getLeafs(root, L)

        // Measure the time it takes to complete the scan
        val clusterStart = System.currentTimeMillis

        // Perform the clustering and write them to a file based on the clusterID
        points.map(p => (p, searchTheIndex(root, null)(p, a))).name("SearchTheIndex")
          .flatMap(new FlatMapper).name("FlatMapper")
          .sortPartition(_._2, Order.ASCENDING).setParallelism(1).name("SortPartition")
          .write(new ClusterOutputFormat(clusterPath), clusterPath.toString, WriteMode.NO_OVERWRITE).name("SinkToCluster")
          .setParallelism(1)

        // Write the leafs to a file to possibly be retrieved later
        env.fromCollection(leafs)
          .write(new SerializedOutputFormat[Point], indexPath.toString, WriteMode.NO_OVERWRITE)
          .setParallelism(1)

        // Force the execution of the clustering, so that the throughput metric is accurate
        env.execute("FileStreaming DeCP - Clustering")

        // Write to the log
        log.info("Clustering the points finished in " + (System.currentTimeMillis - clusterStart) + " milliseconds")

        leafs
      } else {
        // Read the leaf points that was previously written
        env.readFile(new SerializedInputFormat[Point], indexPath.toString).collect.toArray
      }

      // Rebalance the incoming Points to every downstream map slot and perform the index search.
      val knn = queryPoints.name("QueryPoints Source").rebalance
        .map(new KNearestNeighbor(null, leafs, clusterPath, b, k)).name("KNearestNeighbor")
        .map(new StreamingGroundTruth(groundTruth, k)).name("StreamingGroundTruth")

      // Forcing the streamEnv here and again at the writeAsCsv method is not an option,
      // as the above code will be executed twice. The writeAsCsv method is fast enough,
      // that I include the time it takes to write the output in the metric that measures
      // the search or scan time.

      knn
    }
    else throw new Exception("Invalid or missing input parameter --method. " +
      "See documentation for valid options.")

    // Write the output and replace the decimal separator with a comma for easier post-processing.
    knn.writeAsCsv(outputPath.getPath, WriteMode.OVERWRITE, "\n", ";")
      .name("WriteAsCsv")
      .setParallelism(1)

    // Force the final output to be written
    streamEnv.execute("FileStreaming DeCP - kNN")
  }


  /**
    * Checks if the given path contains any files. The file-based streaming method depends
    * on written files and must not see stale files when reclustering.
    *
    * @param clusterPath The path to the clustering directory.
    */
  def checkTargetPath(clusterPath: Path): Unit = {
    // Check if the clustered files from previous tests were not deleted
    val hdfsPath = new org.apache.hadoop.fs.Path(clusterPath.toString)
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()
    val fileSystem = hdfsPath.getFileSystem(hdfsConfig)
    if (!fileSystem.listStatus(hdfsPath).isEmpty)
      throw new Exception("Error: ClusterPath is not empty before performing clustering.")
  }
}

