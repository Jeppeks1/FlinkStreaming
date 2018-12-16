import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{createTypeInformation, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.util.Collector

import org.slf4j.{Logger, LoggerFactory}

import container.InternalNode._
import container.Point
import functions._
import reader._


/**
  * The DeCP object contains the batch implementation of the DeCP pipeline. The implementation uses
  * the same index construction technique as in the streaming version, but utilizes a different strategy
  * to connect the points and query points. The set of query points is much smaller than the base points
  * and is suitable for broadcasting to the downstream operators. The data is connected using the join
  * operator and compared to the ground truth to obtain an accuracy measure.
  */
object BatchDeCP {

  private val log: Logger = LoggerFactory.getLogger("DeCP")
  private val recordSize: Int = 128 + 8 // 128 floats of one byte each, eight bytes from the Long pointID

  /**
    * Usage:
    * {{{
    *   StreamingDeCP --sift <String>
    *                 --method <String>
    *                 --recluster <Boolean>
    *                 --reduction <Int>
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
    val method = params.getRequired("method")
    val sift = params.getRequired("sift")
    val L = params.get("L", "2").toInt
    val a = params.get("a", "1").toInt
    val b = params.get("b", "5").toInt
    val k = params.get("k", "100").toInt
    val reduction = params.get("reduction", "1").toInt
    val treeA = params.get("treeA", "3").toInt

    // Set the paths and configuration properties
    // val siftPath = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftmedium\\"
    val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/" + sift
    val ext = if (sift == "siftlarge") ".bvecs" else ".fvecs"
    val truthPath = if (sift == "siftlarge") "/truth/idx_" + 1000 / reduction + "M.ivecs" else "/truth/groundtruth.ivecs"

    val featureVectorPath = new Path(siftPath + "/base" + ext)
    val groundTruthPath = new Path(siftPath + truthPath)
    val queryPointPath = new Path(siftPath + "/query" + ext)
    val outputPath = new Path(siftPath + "/output.csv")

    // Get the ExecutionEnvironments and read the data using a PointInputFormat
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val queryPoints: DataSet[Point] = env.readFile(new PointInputFormat(queryPointPath, 1), queryPointPath.toString)
    val points: DataSet[Point] = env.createInput(new PointInputFormat(featureVectorPath, reduction)).name("Point Source")
    val truth: DataSet[(Int, Array[Int])] = env.createInput(new TruthInputFormat(groundTruthPath)).setParallelism(4)

    val knn = if (method == "scan") {
      // Perform a full sequential scan
      val knn = points.flatMap(new BatchSequentialScan)
        .withBroadcastSet(queryPoints, "queryPoints")
        .groupBy(_._1) // Group by queryPointID
        .sortGroup(_._3, Order.ASCENDING) // Sort by distance
        .first(k) // Select the first k elements in each group. Re-grouping is necessary.
        .groupBy(_._1) // Re-grouping is unfortunate, but massively reduces the dataflow to the next operator
        .reduceGroup(new ResultCollector)

      knn
    }
    else if (method == "index") {
      // Build the root node of the index for the index search and get the leafs
      val root = buildTheIndex(points, recordSize, treeA, L)
      val leafs = getLeafs(root, L)

      // Perform the clustering
      val cluster = points
        .flatMap(new ClusterWithIndex(null, a))
        .withBroadcastSet(env.fromElements(root), "root")
        .name("ClusterWithIndex")

      // Discover the clusterID of each query point
      val flattenedQueryPoints = queryPoints
        .flatMap { (p, col: Collector[(Point, Long)]) =>
          val slice = leafs.map(l => (l.pointID, l.eucDist(p))).sortBy(_._2).slice(0, b)
          slice.foreach(in => col.collect(p, in._1))
        }.name("LeafScan")

      // Join the clustered points with the query points and find kNN
      val knn = cluster
        .join(flattenedQueryPoints)
        .where(_._2) // clusterID from cluster
        .equalTo(_._2).name("JoinOnClusterID") // clusterID from flattenedQueryPoints
        .map(in => (in._2._1.pointID, in._1._1.pointID, in._2._1.eucDist(in._1._1)))
        .name("ScanWithinCluster") // (queryPointID, clusterPointID, distance)
        .distinct // Remove duplicate tuples (relevant if b > 1)
        .groupBy(_._1) // Group by queryPointID
        .sortGroup(_._3, Order.ASCENDING) // Sort by distance
        .first(k) // Select the first k elements in each group
        .groupBy(_._1) // Re-grouping is unfortunate, but first(k) massively reduces the dataflow to the next operator
        .reduceGroup(new ResultCollector)

      knn
    }
    else throw new Exception("Invalid or missing input parameter --method. " +
      "See documentation for valid options.")

    // Write the output
    knn.map(new GroundTruth(k))
      .withBroadcastSet(truth, "groundTruth")
      .writeAsCsv(outputPath.getPath, "\n", ";", WriteMode.OVERWRITE)
      .name("SinkToCSV")
      .setParallelism(1)

    // Execute the job
    env.execute("Batch DeCP")
  }

}


