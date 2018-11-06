package project.distributed.container

import java.io.Serializable

import scala.math.{ceil, pow}
import scala.util.Random

/**
  * An InternalNode represents all the nodes of the index tree
  * and is used to guide the incoming points towards the correct leaf.
  *
  * @param children  The set of nodes that is accessible from this node.
  * @param pointNode The point of the designated cluster leader
  *              at the current level.
  */
case class InternalNode(var children: Vector[InternalNode],
                        var pointNode: Point) extends Serializable {

  def isLeaf: Boolean = if (children.isEmpty) true else false

  override def toString: String = "[" + pointNode.pointID + "] " + children.map(_.pointNode.pointID)
}


object InternalNode {

  var inputSize: Long = _
  var levels: Int = 3
  var treeA: Int = _
  val balancingFactor: Double = 0.0


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
    // at the current level is higher than the count on the level before. Adjust for this case by
    // setting a default reduction factor of 40 percent.
    if (leaderCount > size) leaderCount = ceil(size * 0.6).toInt

    val rng = new Random(12)
    var vec = Vector.fill(leaderCount)(points(rng.nextInt(size.toInt)))

    while (vec.distinct.size != leaderCount) {
      vec = vec :+ points(rng.nextInt(size.toInt))
    }

    val vecDistinct = vec.distinct
    val parents = points.map{in => (in, findParents(vecDistinct)(in.pointNode))}
    vecDistinct.map(in => findChildren(parents)(in.pointNode))
  }


  /**
    * Finds the treeA closest parent nodes for the incoming point.
    * @param current Vector of InternalNodes at the current level.
    * @param point A point belonging to a Node at the previous level.
    * @return The treeA closest parent nodes.
    */
  def findParents(current: Vector[InternalNode])(point: Point): Vector[Long] = {
    current
      .map(in => (in, in.pointNode.eucDist(point)))
      .sortBy(_._2)
      .slice(0, InternalNode.treeA)
      .map(_._1.pointNode.pointID)
    // We need the pointID for a comparison in the findChildren method
  }


  def findChildren(parents: Vector[(InternalNode, Vector[Long])])(point: Point): InternalNode = {
    val search = parents.collect{
      case x if x._2.contains(point.pointID) => x._1
    }
    InternalNode(search.asInstanceOf[Vector[InternalNode]], point)
  }


  def pickRandomLeafs(points: Vector[Point], levelsIn: Int): Vector[InternalNode] = {
    val test = points.size
    setVariables(test, levelsIn)
    val size = inputSize
    val IOGranularity = 128 * 1024 // 128 KB - based on Linux OS
    val descriptorSize = points.head.descriptor.size * 4  // 4 bytes per integer or float
    //val leaderCount = ceil(size / floor(IOGranularity / descriptorSize)).toInt
    val leaderCount = ceil(pow(size, 1 - 1.0/(levels + 1))).toInt
    val balancedLeaderCount = ceil(leaderCount + leaderCount * balancingFactor).toInt

    // Cheating and picking non-random points for simplicity. TODO: Fix
    points.take(balancedLeaderCount).map(InternalNode(Vector(), _))
  }

}

