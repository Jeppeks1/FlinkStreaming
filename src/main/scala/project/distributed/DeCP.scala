package project.distributed

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.functions._
import org.apache.flink.util.Collector
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.createTypeInformation // This explicit import is required for some reason

import org.slf4j.{Logger, LoggerFactory}

import project.distributed.container.{InternalNode, Point}
import project.distributed.reader.FeatureVector._
import project.distributed.reader.PointInputFormat
import project.distributed.functions._
import project.distributed.container.IndexTree._
import org.apache.flink.core.fs.Path

import scala.collection.JavaConverters._


/**
  * The DeCP object contains the batch implementation of the DeCP pipeline. The implementation uses
  * the same index construction technique as in the streaming version, but utilizes a different strategy
  * to connect the points and query points. The set of query points is much smaller than the base points
  * and is suitable for broadcasting to the downstream operators. The data is connected using the join
  * operator and compared to the ground truth to obtain an accuracy measure.
  */
object DeCP {

  protected val logger: Logger = LoggerFactory.getLogger("StreamingDeCP")

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
    val queryPoints: DataSet[Point] = env.createInput(new PointInputFormat(queryPointPath))
    val points: DataSet[Point] = env.createInput(new PointInputFormat(featureVectorPath))

    // Read the ground truth and determine the number of points in the input
    val groundTruth: DataSet[Vector[Int]] = env.fromCollection(ivecs_truth(groundTruthPath.getPath))
    val inputSize = points.count

    val knn = if (method == "scan") {
      // Perform a full sequential scan
      val knn = points
        .flatMap(new BatchSequentialScan)
        .withBroadcastSet(queryPoints, "queryPoints")
        .groupBy(_._1) // Group by queryPointID
        .sortGroup(_._3, Order.ASCENDING) // Sort by distance
        .first(k) // Select the first k elements in each group
        .groupBy(_._1) // Re-grouping is unfortunate, but massively reduces the dataflow to the next operator
        .reduceGroup(new ResultCollector)

      knn
    }
    else if (method == "index") {

      val (root, cluster) = if (recluster) {

        // Find the leaf nodes
        val leafs = points
          .filter(new SelectRandomLeafs(inputSize, L)) // TODO: Should not be random
          .map(p => (1, InternalNode(Vector(), p)))

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

        // TODO: Write the clustering to a file with a FileOutputFormat

        (root, cp)

      } else {
        throw new Exception("Not yet implemented: recluster = false")
      }

      // Discover the clusterID of each query point
      val qp = queryPoints
        .map(qp => (qp, searchTheIndex(root, null)(qp, b)))
        .flatMap(new FlatMapper)

      // Join the clustered points with the query points and find kNN
      val knn = cluster
        .join(qp)
        .where(1) // clusterID from cluster
        .equalTo(1) // clusterID from queryPoint
        .map(in => (in._2._1.pointID, in._1._1.pointID, in._2._1.eucDist(in._1._1))) // (queryPointID, clusterPointID, distance)
        .distinct // Remove duplicate tuples (relevant if b > 1) TODO: Move this operator up?
        .groupBy(_._1) // Group by queryPointID
        .sortGroup(_._3, Order.ASCENDING) // Sort by distance
        .first(k) // Select the first k elements in each group
        .groupBy(_._1) // Re-grouping is unfortunate, but first(k) massively reduces the dataflow to the next operator
        .reduceGroup(new ResultCollector)

      knn
    }
    else throw new Exception("Invalid or missing input parameter --method. " +
      "See documentation for valid options.")

    val result = knn
      .map(new GroundTruth(k))
      .withBroadcastSet(groundTruth, "groundTruth")
      .map(in => (in._1, in._2, in._3))
      .collect
      .toVector

    val hit = result.map(_._2).sum / result.size
    val count = result.map(_._3).sum / result.size

    println("--- Accuracy ----")
    println("Hit: " + hit)
    println("Recall: " + count)
  }


  final class BatchSequentialScan extends RichFlatMapFunction[Point, (Long, Long, Double)] {

    private var queryPoints: Traversable[Point] = _

    override def open(parameters: Configuration): Unit = {
      queryPoints = getRuntimeContext.getBroadcastVariable[Point]("queryPoints").asScala
    }

    override def flatMap(input: Point, out: Collector[(Long, Long, Double)]): Unit = {
      // For the incoming point, calculate and emit the distance to all query points
      queryPoints.foreach { qp =>
        out.collect((qp.pointID, input.pointID, input.eucDist(qp)))
      }
    }
  }

}


