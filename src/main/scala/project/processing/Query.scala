package project.processing

import project.container.SiftDescriptor
import project.clustering.IndexTree
import scala.collection.immutable.Queue
import scala.util.Random

object Query extends App {

  val points = Vector(
    SiftDescriptor(1,  Vector(1, 1)),
    SiftDescriptor(2,  Vector(1, 3)),
    SiftDescriptor(3,  Vector(2, 2)),
    SiftDescriptor(4,  Vector(8, 6)),
    SiftDescriptor(5,  Vector(8, 8)),
    SiftDescriptor(6,  Vector(9, 7)),
    SiftDescriptor(7,  Vector(1, 8)),
    SiftDescriptor(8,  Vector(2, 8)),
    SiftDescriptor(9,  Vector(2, 9)),
    SiftDescriptor(10, Vector(2, 3)))

  val idxTree = new IndexTree[Int]
  idxTree.buildIndexTree(points, 3, 1)


}
