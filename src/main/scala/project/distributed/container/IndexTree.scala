package project.distributed.container

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
import java.io.Serializable


import scala.util.Random
import scala.math.{ceil, floor, pow}

@SerialVersionUID(123L)
class IndexTree extends Serializable { self =>

  /**
    * A convenience type representing any node in the index tree,
    * which can be either a SearchNode or a Leaf.
    */
  private type Node = Either[Vector[SearchNode], Vector[Leaf]]


  /**
    * A SearchNode represents the non-leaf levels of the index tree
    * and is used to guide the incoming points to the correct Leaf.
    *
    * @param children      The set of nodes that is accessible from this node.
    *                      A SearchNode can either contain another level of
    *                      SearchNodes or Leafs.
    * @param clusterLeader The point of the designated cluster leader
    *                      at the current level.
    */
  private case class SearchNode(children: Node,
                                clusterLeader: Point) extends Serializable {

    def pickRandomNodes(points: Vector[Node], n: Int, seed: Long): Vector[SearchNode] = {
      val size = points.length
      val leaderCount = ceil(pow(size, self.levels / (self.levels + 1))).toInt

      val rng = new Random(seed)
      var vec = Vector.fill(leaderCount)(points(rng.nextInt(size.toInt)))

      while (vec.distinct.size != n) {
        vec = vec :+ points(rng.nextInt(size.toInt))
      }

      vec.flatMap {
        case node@Left(searchNodes) =>
          searchNodes.map(sn => SearchNode(findChildren(points)(node), sn.clusterLeader))
        case node@Right(leafss) =>
          leafss.map(leaf => SearchNode(findChildren(points)(node), leaf.clusterLeader))
      }
    }

    def findChildren(nodes: Vector[Node])(node: Node): Node = {
      val points = nodes.flatMap {
        case Left(searchNodes) => searchNodes.map(_.clusterLeader)
        case Right(leafss) => leafss.map(_.clusterLeader)
      }

      val point = node match {
        case Left(searchNode) => searchNode.head.clusterLeader
        case Right(leaf) => leaf.head.clusterLeader
      }
      val knn = new Cluster(points, point.pointID)
      knn.kNearestNeighbor(point, self.treeA)
      _
    }

  }

  /**
    * A Leaf represents the bottom layer of the index tree.
    * When a SearchNode encounters a Leaf, the search is finished
    * and the incoming points should be assigned to the cluster
    * with the same clusterID as the ID of the clusterLeader.
    *
    * @param clusterLeader The point of the designated cluster leader
    *                      at the Leaf level.
    */
  private case class Leaf(clusterLeader: Point) extends Serializable {

    def pickRandomLeafs(points: Vector[Point], n: Int, seed: Long): Vector[Leaf] = {
      val size = points.length
      val IOGranularity = 128 * 1024 // 128 KB - based on Linux OS
      val descriptorSize = size * 4 // 4 bytes per integer or float
      val leaderCount = ceil(size / floor(IOGranularity / descriptorSize)).toInt

      val rng = new Random(seed)
      var vec = Vector.fill(leaderCount)(points(rng.nextInt(size.toInt)))

      while (vec.distinct.size != n) {
        vec = vec :+ points(rng.nextInt(size.toInt))
      }
      vec.map(Leaf)
    }
  }

  // Defining member variables
  var leafs: Vector[Leaf] = _
  var root: SearchNode = _
  var levels: Int = _
  var treeA: Int = _


  def buildIndexTree(points: Vector[Point], L: Int, a: Int): Unit = {

    // TODO: Set member variables


  }

  //  def toInternalNode(points: Vector[IndexNode],
  //                     leafs: Option[Vector[IndexNode]],
  //                     children: Vector[IndexNode],
  //                     level: Int): Vector[IndexNode] = {
  //    points.map(SD => IndexNode(children, new Cluster(SD), level))
  //  }


}



