package project.local.container

import java.io.Serializable

import scala.math.{ceil, floor, pow}
import scala.util.Random

@SerialVersionUID(123L)
class IndexTree extends Serializable { self =>

  /**
    * An InternalNode represents the all nodes of the index tree
    * and is used to guide the incoming points towards the correct leaf.
    *
    * @param children      The set of nodes that is accessible from this node.
    * @param clusterLeader The point of the designated cluster leader
    *                      at the current level.
    */
  private case class InternalNode(children: Vector[InternalNode],
                                  clusterLeader: Point) extends Serializable {

    def pickRandomNodes(points: Vector[InternalNode], n: Int, seed: Long): Vector[InternalNode] = {
      val size = points.length
      val leaderCount = ceil(pow(size, self.levels / (self.levels + 1))).toInt

      val rng = new Random(seed)
      var vec = Vector.fill(leaderCount)(points(rng.nextInt(size.toInt)))

      while (vec.distinct.size != n) {
        vec = vec :+ points(rng.nextInt(size.toInt))
      }

      vec.map{p => InternalNode(findChildren(points)(p.clusterLeader), p.clusterLeader)}

    }

    // Cannot re-use the k-NN algorithm from the clusters, as the children info would be lost
    def findChildren(points: Vector[InternalNode])(point: Point): Vector[InternalNode] = {
      points.map(in =>
        InternalNode(in.children, in.clusterLeader.eucDist(point))).sorted.slice(0, self.treeA)
    }


    def pickRandomLeafs(points: Vector[Point], n: Int, seed: Long): Vector[InternalNode] = {
      val size = points.length
      val IOGranularity = 128 * 1024 // 128 KB - based on Linux OS
      val descriptorSize = size * 4 // 4 bytes per integer or float
      val leaderCount = ceil(size / floor(IOGranularity / descriptorSize)).toInt

      val rng = new Random(seed)
      var vec = Vector.fill(leaderCount)(points(rng.nextInt(size.toInt)))

      while (vec.distinct.size != n) {
        vec = vec :+ points(rng.nextInt(size.toInt))
      }
      vec.map(p => InternalNode(null, p))
    }

  }


  // Defining member variables
  var leafs: Vector[Point] = _
  var root: InternalNode = _
  var levels: Int = _
  var treeA: Int = _


  def buildIndexTree(points: Vector[Point], L: Int, a: Int): Unit = {

    // TODO: Set member variables


  }


}



