package project.distributed.container

import java.io.Serializable
import org.apache.flink.api.scala._

import scala.math.{ceil, floor, pow}
import scala.util.Random

/**
  * An InternalNode represents all the nodes of the index tree
  * and is used to guide the incoming points towards the correct leaf.
  *
  * @param children      The set of nodes that is accessible from this node.
  * @param clusterLeader The point of the designated cluster leader
  *                      at the current level.
  */
case class InternalNode(children: Vector[InternalNode],
                        clusterLeader: Point) extends Serializable {

}


object InternalNode {

  var levels: Int = 3
  var treeA: Int = 3
  var inputSize: Long = _

  def setVariables(_inputSize: Long, _levels: Int): Unit = {
    levels = _levels
    inputSize = _inputSize
  }


  def pickRandomNodes(points: Vector[InternalNode],
                      level: Int,
                      seed: Long): Vector[InternalNode] = {
    val size = points.length
    var leaderCount = ceil(pow(inputSize, 1 - level.toDouble / (levels.toDouble + 1))).toInt

    // The disrepancy between methods in eCP and DeCP gives rises to cases, where the leader count
    // at the current level, higher than the count on the level before. Adjust for this case by
    // setting a default reduction factor of 40 percent.
    if (leaderCount > size) leaderCount = ceil(size * 0.6).toInt

    val rng = new Random(12)
    var vec = Vector.fill(leaderCount)(points(rng.nextInt(size.toInt)))

    while (vec.distinct.size != leaderCount) {
      vec = vec :+ points(rng.nextInt(size.toInt))
    }

    vec.distinct.map { p => InternalNode(findChildren(points)(p.clusterLeader), p.clusterLeader) }
  }

  // Cannot re-use the k-NN algorithm from the clusters, as the children info would be lost
  def findChildren(points: Vector[InternalNode])(point: Point): Vector[InternalNode] = {
    points.map(in =>
      InternalNode(in.children, in.clusterLeader.eucDist(point))).sorted.slice(0, InternalNode.treeA)
  }


  def pickRandomLeafs(points: DataSet[Point], levelsIn: Int): Vector[InternalNode] = {
    setVariables(points.count, levelsIn)
    val size = inputSize
    val IOGranularity = 128 * 1024 // 128 KB - based on Linux OS
    val descriptorSize = points.first(1).collect.head.descriptor.size * 4  // 4 bytes per integer or float
    //val leaderCount = ceil(size / floor(IOGranularity / descriptorSize)).toInt
    val leaderCount = ceil(pow(size, 1 - 1.0/(levels + 1))).toInt

    // Cheating and picking non-random points for simplicity. TODO: Fix
    points.first(leaderCount).collect.toVector.map(InternalNode(null, _))
  }



  implicit def orderByDistance[A <: InternalNode]: Ordering[InternalNode] =
    Ordering.by(_.clusterLeader.distance)
}

