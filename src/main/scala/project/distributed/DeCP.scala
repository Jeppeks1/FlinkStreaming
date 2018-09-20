package project.distributed

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.operators.Order
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import project.distributed.reader.FeatureVector._
import project.distributed.container.InternalNode
import project.distributed.container.IndexTree._
import project.distributed.container.Point

import scala.collection.JavaConverters._
import java.lang.Iterable

object DeCP {

  /**
    *
    * Usage:
    * {{{
    *   DeCP --method <String>
    *        --featureVector <path>
    *        --queryPoint <path>
    *        --L <Int>
    *        --a <Int>
    *        --b <Int>
    *        --k <Int>
    * }}}
    *
    */
  def main(args: Array[String]): Unit = {
    // Initialize input and variables
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val method = params.get("method")
    val k = params.get("k").toInt

    // Get the execution environment and read the data
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val queryPoints: DataSet[Point] = readQueryPoints(env, params).first(3)
    val points: DataSet[Point] = readFeatureVector(env, params).filter(_.pointID > 2)
    val root: InternalNode = buildIndexTree(points, params)

    // The set of all points grouped on the cluster they belong to.
    // Will later be used to retrieve the relevant set of clusters
    // and perform a kNN search within them.
    val clusteredPoints = points
      .map(p => (p, searchIndex(root)(p).pointID))
      .groupBy(1)

    // Process the query points
    if (method == "scan") {

      // Perform a full sequential scan
      clusteredPoints
        .reduceGroup(new SequentialScan)
        .withBroadcastSet(queryPoints, "queryPoints")
        .groupBy(0)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new kNearestNeighbors(k))
        .print

    }
    else if (method == "index") {
      // Discover which clusters to search through.
      val query2cluster = queryPoints
        .map(qp => (qp, searchIndex(root)(qp).pointID))

      // Search through the relevant clusters.
      clusteredPoints
        .reduceGroup(new FindDistances)
        .withBroadcastSet(query2cluster, "query2cluster")
        .groupBy(0)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new kNearestNeighbors(k))
        .print
    }
  }

  /**
    * The SequentialScan class takes the clustered input points on the format (Point, ClusterID)
    * and reduces
    */
  final class SequentialScan extends RichGroupReduceFunction[(Point, Long), (Long, Long, Double)] {
    private var queryPoints: Traversable[Point] = _

    override def open(parameters: Configuration): Unit =
      queryPoints = getRuntimeContext.getBroadcastVariable[Point]("queryPoints").asScala

    def reduce(it: Iterable[(Point, Long)],
               out: Collector[(Long, Long, Double)]): Unit = {
      it.iterator().asScala.foreach { pl => // For each (Point, clusterID) in the incoming group
        queryPoints.foreach { qp => // For each query point
          out.collect(qp.pointID, pl._1.pointID, pl._1.eucDist(qp).distance)
        }
      }
    }
  }


  final class FindDistances extends RichGroupReduceFunction[(Point, Long), (Long, Long, Double)] {
    private var query2cluster: Traversable[(Point, Long)] = _

    override def open(parameters: Configuration): Unit =
      query2cluster = getRuntimeContext.getBroadcastVariable[(Point, Long)]("query2cluster").asScala

    def reduce(it: Iterable[(Point, Long)],
               out: Collector[(Long, Long, Double)]): Unit = {
      it.iterator().asScala.foreach(pl =>
        query2cluster.foreach { ql =>
          if (ql._2 == pl._2) {
            val point = pl._1.eucDist(ql._1)
            out.collect((ql._1.pointID, point.pointID, point.distance))
          }
        })
    }
  }

  final class kNearestNeighbors(k: Int) extends RichGroupReduceFunction[(Long, Long, Double), (Long, List[Long], Double)] {

    def reduce(it: Iterable[(Long, Long, Double)],
               out: Collector[(Long, List[Long], Double)]): Unit = {
      val group = it.iterator().asScala.toList
      out.collect((group.head._1, group.map(_._2).take(k), group.head._3))
    }
  }


}
