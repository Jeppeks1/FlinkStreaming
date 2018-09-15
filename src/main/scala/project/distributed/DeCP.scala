package project.distributed

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._

import project.distributed.reader.FeatureVector._
import project.distributed.container.Point
import project.distributed.container.Cluster

import project.distributed.container.eCPALTree
import project.distributed.container.SiftDescriptorContainer
import project.distributed.container.SiftKnnContainer
import scala.math.{ceil, floor}
import scala.util.Random

object DeCP {

  /**
    *
    * Usage:
    * {{{
    *   DeCP --featureVector <path> --L <Int> --a <Int>
    * }}}
    *
    */
  def main(args: Array[String]): Unit = {
    // Initialize input, environment and data points

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val points: Vector[Point] = readFeatureVector(params)


    // For testing
    var idx = 0
    var container = Array[SiftDescriptorContainer]()
    for (el <- points){
      container = container :+ new SiftDescriptorContainer(idx, el.descriptor.map(_.toByte).toArray)
      idx = idx + 1
    }


    val tree = new eCPALTree
    tree.buildIndexTreeFromLeafs(2,1,container)
    val test = tree.getTopStaticTreeCluster(container(18), 8) // Get 8-nn to query point with id = 18



  }





}
