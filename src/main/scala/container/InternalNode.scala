package container

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala._
import functions.{FindChildren, FindParents, SelectRandomLeafs, SelectRandomNodes}

import org.slf4j.{Logger, LoggerFactory}

import scala.math.{ceil, floor, pow}

/**
  * The basic structure that represents an index tree. The InternalNode can
  * be used to efficiently guide the incoming data points to their nearest
  * cluster, so that only a fraction of all the data points will be considered
  * at query time, when looking for the k-nearest neighbors of a query point.
  *
  * @param children  The set of nodes that is accessible from this node.
  * @param pointNode The point of the designated cluster leader
  *                  at the current level.
  */
case class InternalNode(var children: Array[InternalNode],
                        var pointNode: Point,
                        var level: Int) extends Serializable {

  /**
    * Flink requires a default constructor to infer the POJO type and avoid GenericType.
    * A default constructor creates an instance of the class from no arguments.
    */
  def this(){
    this(Array[InternalNode](), new Point(-1, Array[Float]()), -1)
  }

  /**
    * Convenience method to override the .toString behaviour for easier debugging.
    * @return A string representation of the InternalNode.
    */
  override def toString: String = "[" + pointNode.pointID + "] " + children.toVector.map(_.pointNode.pointID)


}


object InternalNode {

  private val log: Logger = LoggerFactory.getLogger(classOf[InternalNode])

  /**
    * Builds the index based on the `points` with height `L` and `treeA` internal
    * connections between child and parent nodes. The record size is only used for
    * logging purposes.
    *
    * @param points DataSet containing the base Points.
    * @param recordSize The number of bytes in a single record.
    * @param treeA  The number of parent nodes to connect each node at the current
    *               level to.
    * @param L      The height of the index.
    * @return The root node of the index.
    */
  def buildTheIndex(points: DataSet[Point], recordSize: Int, treeA: Int, L: Int): InternalNode = {
    // Get the size of the input points and set the descriptor size for the file-based case
    val inputSize = points.count

    // Measure the time it takes to build the index
    val rootStart = System.currentTimeMillis

    // Find the leaf nodes
    val leafs = points
      .filter(new SelectRandomLeafs(inputSize, recordSize, L)).name("SelectRandomLeafs")
      .map(p => InternalNode(Array(), p, 0)).name("InternalNode Wrapper")

    if (L == 1) {
      // Only the leafs are to be returned. Collect them and wrap them in an InternalNode.
      val leafsColl = leafs.collect.toArray
      val rootNode = InternalNode(leafsColl, leafsColl.head.pointNode, L)

      // Write to the log and return the result
      log.info("Finished building the index in " + (System.currentTimeMillis - rootStart) + " milliseconds")
      return rootNode
    }

    // Build the root node
    val rootNode = leafs.iterate(L - 1) { currentNodes =>
      // Select new nodes that will make up the nodes of the next level
      val nextLevel = currentNodes
        .filter(new SelectRandomNodes(inputSize, L)).name("SelectRandomNodes")
        .withBroadcastSet(currentNodes, "currentNodes")

      // For every node in the current level, find the treeA nearest nodes at the next level
      val parentNodes = currentNodes
        .map(new FindParents(treeA)).name("FindParents")
        .withBroadcastSet(nextLevel, "newNodes")

      // Discover the nodes at the previous level, which is nearest to the new node
      val nodes = nextLevel
        .map(new FindChildren).name("FindChildren")
        .withBroadcastSet(parentNodes, "parentNodes")

      nodes
    }.collect.toArray

    // Wrap the nodes in a single InternalNode representing the root
    val root = InternalNode(rootNode, rootNode(0).pointNode, L)

    // Write to the log
    log.info("Finished building the index in " + (System.currentTimeMillis - rootStart) + " milliseconds")
    for (level <- 0 to L)
      log.info("The size of the index at level " + level + " is " + actualIndexSize(root, level, L) + "/" +
        expectedIndexSize(inputSize, recordSize, level, L))

    root
  }


  /**
    * Searches the index for the cluster leaders that is closest to the
    * given input point and returns the nearest a points as found by
    * comparing distances. This method should be used when clustering.
    *
    * @param current The current InternalNode of the index which can be
    *                obtained from the method buildIndexTree.
    * @param point Point used as reference in the search.
    * @param a The number of clusters to use for redundant clustering.
    * @note The search does not always return the optimal result.
    * @return The nearest cluster leader of the index leafs to the
    *         input point.
    */
  def clusterWithIndex(current: InternalNode, point: Point, a: Int): Array[InternalNode] = {
    if (current.level == 1) {
      current.children.map(in =>
        (in, in.pointNode.eucDist(point))).sortBy(_._2).map(_._1).slice(0, a)
    }
    else {
      // Distances are sorted and selected using .minBy
      val nearestChild = current.children.map(in =>
        (in, in.pointNode.eucDist(point))).minBy(_._2)._1

      clusterWithIndex(nearestChild, point, a)
    }
  }


  /**
    * Searches the index for the cluster leaders that is closest to the
    * given input point and returns the nearest b points as found by
    * comparing distances. This method should be used for guiding the
    * query points to the clusters.
    *
    * @param current The current InternalNode of the index which can be
    *                obtained from the method buildIndexTree.
    * @param previous The previous InternalNode at the level before the current
    *                 level.
    * @param point Point used as reference in the search.
    * @param b The number of clusters for extended searches.
    * @note The search does not always return the optimal result.
    * @return The nearest cluster leader of the index leafs to the
    *         input point.
    */
  def searchTheIndex(current: InternalNode, previous: InternalNode)(point: Point, b: Int): Array[Long] = {
    if (current.level == 1) {
      // Ensure the search is wide enough
      if (current.children.length < b && previous != null){
        // Widen the search to consider all leaf nodes reachable from the previous node
        indexRecursion(previous, 0, previous.level).map(in =>
          (in, in.pointNode.eucDist(point))).sortBy(_._2).map(_._1.pointNode.pointID).distinct.slice(0, b)
      }
      else {
        // Find the nearest cluster leaders amongst the leaf nodes
        current.children.map(in =>
          (in, in.pointNode.eucDist(point))).sortBy(_._2).map(_._1.pointNode.pointID).slice(0, b)
      }
    }
    else {
      // Distances are sorted and selected using .minBy
      val nearestChild = current.children.map(in =>
        (in, in.pointNode.eucDist(point))).minBy(_._2)._1

      searchTheIndex(nearestChild, current)(point, b)
    }
  }


  /**
    * Finds the treeA closest parent nodes for the incoming point.
    * @param current Vector of InternalNodes at the current level.
    * @param treeA Parameter determining the number of closest parent
    *              to be returned.
    * @param point A point belonging to a Node at the previous level.
    * @return The treeA closest parent nodes.
    */
  def findParents(current: Array[InternalNode], treeA: Int)(point: Point): Array[Long] = {
    current
      .map(in => (in, in.pointNode.eucDist(point)))
      .sortBy(_._2)
      .slice(0, treeA)
      .map(_._1.pointNode.pointID)
    // We need the pointID for a comparison in the findChildren method
  }


  /**
    * Finds and sets the children of incoming Point.
    *
    * @param parents A mapping from an InternalNode at the current level to the treeA
    *                nearest pointIDs at the next level.
    * @param point The point at the next level for which to discover the children.
    * @return An InternalNode at the next level with the children set.
    */
  def findChildren(parents: Array[(InternalNode, Array[Long])])(point: Point): InternalNode = {
    val search = parents.collect{
      case x if x._2.contains(point.pointID) => x._1
    }

    val nextLevel = parents.head._1.level + 1 // All levels should be the same, so we pick the head
    InternalNode(search.asInstanceOf[Array[InternalNode]], point, nextLevel)
  }


  /**
    * Find the expected size of the index at level L which is only used for easy
    * logging of the index size. Note that the recordSize varies depending on if
    * the file-based clustering is uses, which requires more space.
    *
    * @param inputSize Number of points in the static input.
    * @param recordSize The size of a single Point record in bytes.
    * @param level The level of interest in the index. One is the leaf level.
    * @param L The depth of the index not including the root.
    * @return The expected size of the index at level 'level'.
    */
  def expectedIndexSize(inputSize: Long, recordSize: Int, level: Int, L: Int): Int = {
    // Set some constants used in the eCP approach
    val balancingFactor = 0        // Pick an additional balancingFactor % leafs
    val IOGranularity = 128 * 1024 // 128 KB IO granularity on a Linux OS

    // Determine the leaderCount based on the CP approach (for debugging) or
    // the eCP approach (for the serious experiments).
    val leaderCounteCP = ceil(inputSize / floor(IOGranularity / recordSize)).toInt
    val leaderCountCP = ceil(pow(inputSize, 1 - level.toDouble / (L.toDouble + 1))).toInt

    if (level == 1) leaderCounteCP else leaderCountCP
  }


  /**
    * Finds the actual size of the index at the level `level`.
    * Only used for easy logging of the index size.
    *
    * @param root The InternalNode designated as the root.
    * @param level The level of interest in the index. Zero is the leaf level.
    * @param L The depth of the index not including the root.
    * @note The groupBy operator in the expression simulates the
    *       .distinct method.
    * @return The actual size of the index at level 'level'.
    */
  def actualIndexSize(root: InternalNode, level: Int, L: Int): Int = {
    indexRecursion(root, level, L).groupBy(_.pointNode.pointID).map(_._2.head).size
  }


  /**
    * Returns a mapping from `clusterID -> count` which represents
    * the number of ways a leaf can be reached in an index search.
    *
    * @param root The InternalNode designated as the root.
    * @param L The depth of the index not including the root.
    * @return a collection containing a map from clusterID -> count
    */
  def pathsToLeaves(root: InternalNode, L: Int): Map[Long, Int] = {
    indexRecursion(root, 0, L).groupBy(_.pointNode.pointID).mapValues(_.length)
  }

  /**
    * Finds the leafs of the InternalNode.
    *
    * @param root The InternalNode designated as the root.
    * @param L The depth of the index not including the root.
    * @note The groupBy operator in the expression simulates the
    *       .distinct method.
    * @return the leafs of the given InternalNode.
    */
  def getLeafs(root: InternalNode, L: Int): Array[Point] = {
    indexRecursion(root, 0, L).groupBy(_.pointNode.pointID).map(_._2.head.pointNode).toArray
  }

  /**
    * Traverses the index and returns the set of Points at the
    * level `level`. The number of duplicate Points returned
    * represents the number of possible ways the Point can be
    * reached in a index search.
    *
    * @param root The InternalNode designated as the root.
    * @param level The level of interest in the index. Zero is the leaf level.
    * @param L The depth of the index not including the root.
    * @return All the InternalNodes at level `level` including duplicate entries
    *         representing the number of distinct paths that can be taken
    *         to reach the given node.
    */
  private def indexRecursion(root: InternalNode, level: Int, L: Int): Array[InternalNode] = {
    if (root.level == level)
      Array(root)
    else
      root.children.flatMap(indexRecursion(_, level, L))
  }

}

