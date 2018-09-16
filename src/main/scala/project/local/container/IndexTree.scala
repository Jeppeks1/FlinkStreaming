package project.local.container

import java.io.Serializable
import project.local.container.InternalNode._

import scala.math.{ceil, floor, pow}
import scala.util.Random

/**
  * The basic structure that represents an index tree. The index tree can
  * be used to efficiently guide the incoming data points to their nearest
  * cluster, so that only a fraction of all the data points will be considered
  * at query time, when looking for the k-nearest neighbors of a query point.
  */
@SerialVersionUID(123L)
class IndexTree extends Serializable {

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
    * @param L      Depth of the index tree.
    * @return Root node of the index.
    */
  def buildIndexTree(points: Vector[Point], L: Int): InternalNode = {

    // Set the static variables instead of passing them in explicitly. TODO: Fix?
    setVariables(points.size, L)

    // Set the random leafs
    leafs = pickRandomLeafs(points, System.currentTimeMillis())
    var current = leafs

    // If the depth of the index is 1, return the root node with just the leafs.
    if (L == 1) return InternalNode(leafs, leafs(0).clusterLeader)

    println("Index size at level 1: " + leafs.size)
    for (level <- 2 to L) {
      current = pickRandomNodes(current, level, System.currentTimeMillis())
      println("Index size at level " + level + ": " + current.size)
    }

    // Return the InternalNodes at level L and wrap them in a root node
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

    if (root.children == null) root.clusterLeader
    else {
      // The call to .min uses the default ordering defined in
      // the companion object of the InternalNode class.
      val nearestChild = root.children.map(in =>
        InternalNode(in.children, in.clusterLeader.eucDist(point))).min

      searchIndex(nearestChild)(point)
    }

  }


}



