package project.distributed.container


import org.apache.flink.api.java.utils.ParameterTool
import project.distributed.container.InternalNode._
import org.apache.flink.api.scala._
import java.io.Serializable

import scala.math.{ceil, floor, pow}
import scala.util.Random

/**
  * The basic structure that represents an index tree. The index tree can
  * be used to efficiently guide the incoming data points to their nearest
  * cluster, so that only a fraction of all the data points will be considered
  * at query time, when looking for the k-nearest neighbors of a query point.
  */
@SerialVersionUID(123L)
object IndexTree extends Serializable {


  // Defining member variables
  var leafs: Vector[InternalNode] = _
  var root: InternalNode = _
  var levels: Int = _
  var treeA: Int = _


  /**
    * Builds an index tree from the input points by choosing random cluster
    * leaders at every level of the index.
    *
    * @param points The initial vector containing all the Points from
    *               which to build an index.
    * @return Root node of the index.
    */
  def buildIndexTree(points: DataSet[Point], params: ParameterTool): InternalNode = {

    // Get the value L from the input
    val L = params.get("L").toInt

    // Set the random leafs
    leafs = pickRandomLeafs(points, L)
    var current = leafs

    // If the depth of the index is 1, return the root node with just the leafs.
    if (L == 1) return InternalNode(leafs, leafs(0).clusterLeader)

    println("Index size at level 1: " + leafs.size)
    for (level <- 2 to L) {
      current = pickRandomNodes(current, level, System.currentTimeMillis())
      println("Index size at level " + level + ": " + current.size)
    }

    // Return the InternalNodes at the top level and wrap them in a root node
    root = InternalNode(current, current(0).clusterLeader)
    root
  }

  /**
    * Searches the index for the cluster leader that is closest to the
    * given input point and returns the nearest point with the distance
    * member set.
    * @param root The root of the index which can be obtained from the
    *             method buildIndexTree.
    * @param point Point used as reference in the search.
    * @note The search does not always return the optimal result.
    * @return The nearest cluster leader of the index leafs to the
    *         input point.
    */
  def searchIndex(root: InternalNode)(point: Point): Point = {

    if (root.isLeaf)
      root.clusterLeader
    else {
      // The call to .min uses the default ordering defined in
      // the companion object of the InternalNode class.
      val nearestChild = root.children.map(in =>
        InternalNode(in.children, in.clusterLeader.eucDist(point))).min

      searchIndex(nearestChild)(point)
    }

  }







  /**
    * Searches the index for the cluster leaders that is closest to the
    * given input point and returns the nearest a points with the distance
    * member set.
    * @param current The current InternalNode of the index which can be
    *                obtained from the method buildIndexTree.
    * @param previous The previous InternalNode at the level before the current
    *                 level.
    * @param point Point used as reference in the search.
    * @param a The number of nearest clusters to return for redundant clustering.
    * @note The search does not always return the optimal result.
    * @return The nearest cluster leader of the index leafs to the
    *         input point.
    */
  def searchTheIndex(current: InternalNode, previous: InternalNode)(point: Point, a: Int): Vector[InternalNode] = {

    if (current.isLeaf){
      previous.children.map(in =>
        InternalNode(in.children, in.clusterLeader.eucDist(point))).sorted.slice(0, a)
    }
    else {
      // The call to .min uses the default ordering defined in
      // the companion object of the InternalNode class.
      val nearestChild = current.children.map(in =>
        InternalNode(in.children, in.clusterLeader.eucDist(point))).min

      searchTheIndex(nearestChild, current)(point, a)
    }

  }


}




