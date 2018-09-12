package project.distributed

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions._
import org.apache.flink.api.scala._

import project.distributed.reader.FeatureVector._
import project.distributed.container.Point


object DeCP {

  def main(args: Array[String]): Unit = {
    // checking input parameters
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // get input data:
    val points: DataSet[Point] = readFeatureVector(params, env)
  }



}
