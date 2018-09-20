package project.distributed

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.operators.Order
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import project.distributed.reader.FeatureVector._
import project.distributed.container.InternalNode
import project.distributed.container.IndexTree._
import project.distributed.container.Cluster
import project.distributed.container.Point

import scala.collection.JavaConverters._
import java.lang.Iterable

object DeCP {

  import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction, RichGroupReduceFunction}


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
    val queryPoints: DataSet[Point] = readQueryPoints(env, params)
    val qp = queryPoints.first(3)
    val points1: DataSet[Point] = readFeatureVector(env, params) // TODO: Broadcast?
    val points = points1.filter(_.pointID > 2)
    val root: InternalNode = buildIndexTree(points, params) // TODO: Broadcast?

    // The set of all points grouped on the cluster they belong to.
    // Will later be used to retrieve the relevant set of clusters
    // and perform a kNN search within them.
    val clusteredPoints = points
      .map(p => (p, searchIndex(root)(p).pointID))
      .groupBy(1)

    // Process the query points
    if (method == "scan") {

      //      queryPoints.first(4)
      //        .map{new KNNScanRich}
      //        .withBroadcastSet(points, "pointsIn")
      //        .print


    }
    else if (method == "index") {

      // Discover which clusters to search through.
      // TODO: Expand this with the parameter b.
      val query2cluster = qp
        .map(qp => (qp, searchIndex(root)(qp).pointID))

      query2cluster.print
      println("Seperator")

      // Search through the relevant clusters.
      clusteredPoints
        .reduceGroup(new FindDistances)
        .withBroadcastSet(query2cluster, "query2cluster")
        .map(qpID_p => (qpID_p.head._1, qpID_p.head._2.pointID, qpID_p.head._2.distance))
        .groupBy(0)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new kNearestNeighbors(k))
        .print
    }
  }

  final class kNearestNeighbors(k: Int) extends RichGroupReduceFunction[(Long, Long, Double), (Long, List[Long], Double)] {

    def reduce(it: Iterable[(Long, Long, Double)],
               out: Collector[(Long, List[Long], Double)]): Unit = {
      var group = it.iterator().asScala.toList
      out.collect((group.head._1, group.map(_._2).take(k), group.head._3))
    }
  }


  final class FindDistances extends RichGroupReduceFunction[(Point, Long), Traversable[(Long, Point)]] {
    private var query2cluster: Traversable[(Point, Long)] = _

    override def open(parameters: Configuration): Unit =
      query2cluster = getRuntimeContext.getBroadcastVariable[(Point, Long)]("query2cluster").asScala

    def reduce(it: Iterable[(Point, Long)],
               out: Collector[Traversable[(Long, Point)]]): Unit = {
      it.iterator().asScala.foreach(pl =>
        // Avoid collecting empty lists
        if (query2cluster.unzip._2.toVector.contains(pl._2))
          out.collect(
            query2cluster.collect { // Filter and distance calculation in one go
              case ql if ql._2 == pl._2 => (ql._2, pl._1.eucDist(ql._1))
            }))
    }
  }




  @ForwardedFields(Array("*->_1"))
  final class KNearestNeighborRich extends RichMapFunction[Point, (Point, Vector[Point])] {
    private var pointsIn: Traversable[Point] = _

    override def open(parameters: Configuration): Unit =
      pointsIn = getRuntimeContext.getBroadcastVariable[Point]("pointsIn").asScala


    def map(queryPoint: Point): (Point, Vector[Point]) = {
      val dataSetIn = ExecutionEnvironment.getExecutionEnvironment.fromCollection(pointsIn.toVector)
      val cluster = new Cluster(dataSetIn, queryPoint)
      val knn = cluster.kNearestNeighbor(queryPoint, 3)
      (queryPoint, knn.collect.toVector)
    }
  }


}
