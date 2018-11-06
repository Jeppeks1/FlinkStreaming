package project.distributed

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.functions._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.createTypeInformation // This explicit import is required for some reason

import org.slf4j.{Logger, LoggerFactory}

import project.distributed.container.{InternalNode, Point}
import project.distributed.reader.FeatureVector._
import project.distributed.functions._
import project.distributed.reader.PointInputFormat
import project.distributed.container.IndexTree._
import org.apache.flink.core.fs.Path


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
  */
object StreamingDeCP {

  val logger: Logger = LoggerFactory.getLogger("StreamingDeCP")
  var clusteredPoints: Vector[(Point, Long)] = _ // Workaround 1 - indexed search
  var groundTruth: Vector[Vector[Int]] = _
  var pointsStatic: Vector[Point] = _ // Workaround 1 - sequential scan

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
    val recluster = params.get("recluster", "false").toBoolean
    val method = params.get("method")
    val sift = params.get("sift")
    val L = params.get("L", "4").toInt
    val a = params.get("a", "1").toInt
    val b = params.get("b", "1").toInt
    val k = params.get("k", "5").toInt
    val treeA = params.get("treeA", "3").toInt

    // Set the paths and configuration properties
    // val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/" + sift + "/" + sift + "_"
    val siftPath = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftsmall\\siftsmall_"
    val featureVectorPath = new Path(siftPath + "base.fvecs")
    val groundTruthPath = new Path(siftPath + "groundtruth.ivecs")
    val queryPointPath = new Path(siftPath + "query.fvecs")
    val clusterPath = new Path(siftPath + "cluster.txt")
    val indexPath = new Path(siftPath + "index.txt")

    // Get the ExecutionEnvironments and read the data using a PointInputFormat
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val queryPoints: DataStream[Point] = streamEnv.readFile(new PointInputFormat(queryPointPath), queryPointPath.toString)
    val points: DataSet[Point] = env.createInput(new PointInputFormat(featureVectorPath))
    val inputSize = points.count

    // Read the ground truth as a normal vector
    groundTruth = ivecs_truth(groundTruthPath.getPath)

    val knn = if (method == "scan") {
      // Use the clusteredPoints variable as a placeholder
      pointsStatic = points.collect().toVector

      // Perform a full sequential scan
      val knn = queryPoints
        .map(new StreamingSequentialScan(k))
        .map(new StreamingGroundTruth(k))

      knn
    }
    else if (method == "index") {

      val (root, cluster) = if (recluster) {

        // Find the leaf nodes
        val leafs = points
          .filter(new SelectRandomLeafs(inputSize, L)) // TODO: Should not be random
          .map(p => (1, InternalNode(Vector(), p)))

        val test = leafs.collect.toVector
        println("Leaf nodes: " + test.map(_._2.pointNode))
        println("Leaf count: " + test.size)

        // Build the root node
        val rootNode = leafs.iterate(L - 1) { currentNodes =>
          // Select new nodes that will make up the nodes of the next level
          val newNodes = currentNodes
            .filter(new SelectRandomNodes(inputSize, L))
            .withBroadcastSet(currentNodes, "currentNodes")

          // For every node in the current level, find the treeA nearest nodes at the next level
          val parentNodes = currentNodes
            .map(new FindParents(treeA))
            .withBroadcastSet(newNodes, "newNodes")

          // Discover the nodes at the previous level, which is nearest to the new node
          val nodes = newNodes
            .map(new FindChildren)
            .withBroadcastSet(parentNodes, "parentNodes")

          nodes
        }.map(_._2).collect.toVector

        val root = InternalNode(rootNode, rootNode(0).pointNode)

        // Perform the clustering
        val cp = points
          .map(p => (p, searchTheIndex(root, null)(p, a)))
          .flatMap(new FlatMapper)

        // First workaround - will likely perform absurdly bad.
        // Uncomment this line for experiments with the other workaround.
        StreamingDeCP.clusteredPoints = cp.collect.toVector

        // TODO: Write the clustering to a file with a FileOutputFormat

        (root, cp)

      } else {
        throw new Exception("Not yet implemented: recluster = false")
      }

      // Discover the clusterID of each query point
      val knn = queryPoints //.filter(_.pointID == 72)
        .map(qp => (qp, searchTheIndex(root, null)(qp, b)))
        .flatMap(new KNearestNeighbor(k))
        .map(new GroundTruth(k))

      knn
    }
    else throw new Exception("Invalid or missing input parameter --method. " +
      "See documentation for valid options.")

    knn.print

    streamEnv.execute("Streaming Environment")

  }
}


