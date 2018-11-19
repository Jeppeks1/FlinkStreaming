package project.distributed

import org.apache.flink.streaming.api.scala._ // This import must be before the createTypeInformation
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import org.apache.flink.api.scala.createTypeInformation // This explicit import is required for some reason
import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.operators.Order

import org.slf4j.{Logger, LoggerFactory}

import project.distributed.container.InternalNode._
import project.distributed.container.InternalNode
import project.distributed.container.Point
import project.distributed.functions._
import project.distributed.reader._


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

  val log: Logger = LoggerFactory.getLogger("project.distributed.FileStreamingDeCP")

  /**
    * Usage:
    * {{{
    *   StreamingDeCP --sift <String>
    *                 --method <String>
    *                 --recluster <Boolean>
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
    val recluster = params.get("recluster", "true").toBoolean
    val method = params.get("method")
    val sift = params.get("sift")
    val L = params.get("L", "4").toInt
    val a = params.get("a", "1").toInt
    val b = params.get("b", "1").toInt
    val k = params.get("k", "5").toInt
    val treeA = params.get("treeA", "3").toInt

    // Set the paths and configuration properties
    val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/" + sift + "/" + sift + "_"
    // val siftPath = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftsmall\\siftsmall_"
    val featureVectorPath = new Path(siftPath + "base.fvecs")
    val groundTruthPath = new Path(siftPath + "groundtruth.ivecs")
    val queryPointPath = new Path(siftPath + "query.fvecs")
    val clusterPath = new Path(siftPath + "cluster/")
    val outputPath = new Path(siftPath + "output.txt")

    // Get the ExecutionEnvironments and read the data using a PointInputFormat
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val queryPoints: DataStream[Point] = streamEnv.readFile(new PointInputFormat(queryPointPath), queryPointPath.toString)
    val points: DataSet[Point] = env.createInput(new PointInputFormat(featureVectorPath))
    val truth: DataSet[(Int, Array[Int])] = env.createInput(new TruthInputFormat(groundTruthPath))
    val inputSize = points.count

    val groundTruth = truth.name("Truth Source").collect.toArray

    val root = if (recluster) {

      // Find the leaf nodes
      val leafs = points.name("Points Source")
        .filter(new SelectRandomLeafs(inputSize, L)).name("SelectRandomLeafs") // TODO: Should not be random
        .map(p => (1, InternalNode(Array(), p))).name("InternalNode Wrapper")

      // Build the root node
      val rootNode = leafs.iterate(L - 1) { currentNodes =>
        // Select new nodes that will make up the nodes of the next level
        val newNodes = currentNodes
          .filter(new SelectRandomNodes(inputSize, L)).name("SelectRandomNodes")
          .withBroadcastSet(currentNodes, "currentNodes")

        // For every node in the current level, find the treeA nearest nodes at the next level
        val parentNodes = currentNodes
          .map(new FindParents(treeA)).name("FindParents")
          .withBroadcastSet(newNodes, "newNodes")

        // Discover the nodes at the previous level, which is nearest to the new node
        val nodes = newNodes
          .map(new FindChildren).name("FindChildren")
          .withBroadcastSet(parentNodes, "parentNodes")

        nodes
      }.map(_._2).name("Recluster Loop").collect.toArray

      val root = InternalNode(rootNode, rootNode(0).pointNode)

      // Perform the clustering and write them to a file based on the clusterID
      points.name("PointsSource")
        .map(p => (p, searchTheIndex(root, null)(p, a))).name("SearchTheIndex")
        .flatMap(new FlatMapper).name("FlatMapper")
        .partitionByHash(_._2).name("PartitionByHash") // Is this necessary?
        .sortPartition(_._2, Order.ASCENDING).name("SortPartition")
        .write(new ClusterOutputFormat(clusterPath), clusterPath.toString, WriteMode.NO_OVERWRITE).name("SinkToCluster")
        .setParallelism(1)

      root
    }
    else {
      throw new Exception("Not yet implemented: recluster = false")
    }

    // Force the execution of the clustering, so that the files are written when the next section is reached
    env.execute("FileStreaming DeCP - Clustering")

    val knn = if (method == "index") {
      // Discover the clusterID of each query point
      val knn = queryPoints.name("QueryPoints Source")
        .map(qp => (qp, System.currentTimeMillis)).name("Latency Metric")
        .map(qp_time => (qp_time._1, qp_time._2, searchTheIndex(root, null)(qp_time._1, b))).name("Index Search")
        .flatMap(new KNearestNeighbor(null, clusterPath, k)).name("KNearestNeighbor")
        .map(new StreamingGroundTruth(groundTruth, k)).name("StreamingGroundTruth")

      knn
    }
    else throw new Exception("Invalid or missing input parameter --method. " +
      "See documentation for valid options.")

    knn
      .writeAsCsv(outputPath.getPath, WriteMode.OVERWRITE)
      .name("WriteAsCsv")
      .setParallelism(1)

    streamEnv.execute("FileStreaming DeCP - kNN")


  }
}

