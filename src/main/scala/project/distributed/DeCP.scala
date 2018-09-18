package project.distributed

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

import project.distributed.reader.FeatureVector._
import project.distributed.container.InternalNode
import project.distributed.container.IndexTree._
import project.distributed.container.Cluster
import project.distributed.container.Point

import scala.collection.JavaConverters._
import scala.util.Random

object DeCP {

  import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}


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
    val points: DataSet[Point] = readFeatureVector(env, params) // TODO: Broadcast?
    val root: InternalNode = buildIndexTree(points, params) // TODO: Broadcast?


    // Process the query points
    if (method == "scan"){

//      queryPoints.first(4)
//        .map{new KNNScanRich}
//        .withBroadcastSet(points, "pointsIn")
//        .print







    }
    else if (method == "index"){

      val clusteredPoints = points
        .map(p => (searchIndex(root)(p).toCluster, p))
        .withForwardedFields("_1->_2") // Input is copied unchanged to position 2 of the output
        .groupBy(0)




    }
  }

  @ForwardedFields(Array("*->_1"))
  final class KNearestNeighborRich extends RichMapFunction[Point, (Point, Vector[Point])]{
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
