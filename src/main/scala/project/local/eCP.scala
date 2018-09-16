package project.local

import project.local.reader.FeatureVector.readFeatureVector
import project.local.container.Point
import project.local.container.IndexTree

import org.apache.flink.api.java.utils.ParameterTool

object eCP {

  def main(args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)
    val points: Vector[Point] = readFeatureVector(params)
    val myTree = new IndexTree
    val root = myTree.buildIndexTree(points, 3)
    val point = myTree.searchIndex(root)(points(55))

    println(point.distance)
    val five = 5
  }

}
