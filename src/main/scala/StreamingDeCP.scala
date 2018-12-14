import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool

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
object StreamingDeCP {

  val log: Logger = LoggerFactory.getLogger("StreamingDeCP")
  val recordSize: Int = 128 * 4 + 4 // 128 floats of four bytes each, 4 bytes from the Long pointID

  /**
    * Usage:
    * {{{
    *   StreamingDeCP --sift <String>
    *                 --method <String>
    *                 --treeA <Int>
    *                 --reduction <Int>
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
    val method = params.getRequired("method")
    val sift = params.getRequired("sift")
    val L = params.get("L", "2").toInt
    val a = params.get("a", "1").toInt
    val b = params.get("b", "5").toInt
    val k = params.get("k", "100").toInt
    val reduction = params.get("reduction", "1").toInt
    val treeA = params.get("treeA", "3").toInt

    // Set the paths and configuration properties
    //     val siftPath = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftsmall\\"
    val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/" + sift
    val ext = if (sift == "siftlarge") ".bvecs" else ".fvecs"
    val truthPath = if (sift == "siftlarge") "/truth/idx_" + 1000/reduction + "M.ivecs" else "/truth/groundtruth.ivecs"

    val featureVectorPath = new Path(siftPath + "/base" + ext)
    val groundTruthPath = new Path(siftPath + truthPath)
    val queryPointPath = new Path(siftPath + "/query" + ext)
    val clusterPath = new Path(siftPath + "/cluster/")
    val outputPath = new Path(siftPath + "/output.csv")

    // Get the ExecutionEnvironments and read the data using a PointInputFormat
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val queryPoints: DataStream[Point] = streamEnv.readFile(new PointInputFormat(queryPointPath, 1), queryPointPath.toString)
    val points: DataSet[Point] = env.createInput(new PointInputFormat(featureVectorPath, reduction)).name("Point Source")
    val truth: DataSet[(Int, Array[Int])] = env.createInput(new TruthInputFormat(groundTruthPath)).setParallelism(4)

    // Collect the ground truth so it can be accessed by the streaming method
    val groundTruth = truth.name("Truth Source").collect.toArray

    val knn = if (method == "scan") {
      // Collect the points so it can be accessed by the streaming method
      val pointsStatic = points.collect.toArray

      // Perform a full sequential scan
      val knn = queryPoints.name("QueryPoints Source").rebalance
        .map(new StreamingSequentialScan(pointsStatic, k)).name("StreamingSequentialScan")
        .map(new StreamingGroundTruth(groundTruth, k)).name("StreamingGroundTruth")

      // Forcing the streamEnv here and again at the writeAsCsv method is not an option,
      // as the above code will be executed twice. The writeAsCsv method is fast enough,
      // that I include the time it takes to write the output in the metric that measures
      // the search or scan time.

      knn
    }
    else if (method == "index") {
      // Build the root node of the index for the index search and get the leafs
      val root = buildTheIndex(points, recordSize, treeA, L)
      val leafs = getLeafs(root, L)

      // Measure the time it takes to cluster the points
      val clusterStart = System.currentTimeMillis

      // Perform the clustering
      val clusteredPoints = points
        .flatMap(new ClusterWithIndex(null, a)).name("ClusterWithIndex")
        .withBroadcastSet(env.fromElements(root), "root")
        .collect
        .toArray

      // Write to the log
      log.info("Clustering the points finished in " + (System.currentTimeMillis - clusterStart) + " milliseconds")

      // Rebalance the incoming Points to every downstream map slot and perform the index search.
      val knn = queryPoints.name("QueryPoints Source").rebalance
        .map(new KNearestNeighbor_Mem(clusteredPoints, null, leafs, clusterPath, b, k)).name("KNearestNeighbor")
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


    log.info("Log me goddammit")

  }
}


