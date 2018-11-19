package container

import org.apache.flink.core.io.IOReadableWritable
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.math.{ceil, floor, pow}
import scala.util.Random

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
                        var pointNode: Point) extends Serializable {

  // Flink requires a default constructor to infer the POJO type and avoid GenericType
  def this(){
    this(Array[InternalNode](), new Point(-1, Array[Float]()))
  }

  /**
    * Checks if the node is a leaf.
    *
    * @return true if the node is a leaf; otherwise false.
    */
  def isLeaf: Boolean = if (children.isEmpty) true else false

  override def toString: String = "[" + pointNode.pointID + "] " + children.toVector.map(_.pointNode.pointID)


//  override def write(out: DataOutputView): Unit = {
//    // Write the root pointNode first
//    out.writeLong(pointNode.pointID)
//    out.write(pointNode.descriptor.map(_.toByte))
//
//    // Write the children recursively
//    children.foreach{child => writeInformation(out, child, pointNode.pointID)}
//  }
//
//  override def read(in: DataInputView): Unit = {
//    // Read the root pointNode and the children
//    pointNode = readPoint(in)
//    children = readInformation(in, Array())
//  }
//
//  def writeInformation(out: DataOutputView, node: InternalNode, parent: Long): Unit = {
//    // Write this pointNode
//    out.writeLong(node.pointNode.pointID)
//    out.write(node.pointNode.descriptor.map(_.toByte))
//
//    node.children.foreach{child => writeInformation(out, child, node.pointNode.pointID)}
//  }
//
//
//  def readInformation(in: DataInputView, children: Array[InternalNode]): Array[InternalNode] = {
//    // Read this pointNode
//    val point = readPoint(in)
//    val parents = 5
//    Array(InternalNode(Array(), point))
//  }
//
//  def readPoint(in: DataInputView): Point = {
//    // Read the serialized pointID
//    val pointID = in.readLong
//
//    // Read the floats into a byte array
//    val bytes = new Array[Byte](128)
//    val bytesRead = in.read(bytes)
//
//    // Make sure something was read
//    assert(bytesRead > 0)
//
//    // Convert the bytes to floats and re-create the point
//    val descriptor = bytes.map(_.toFloat)
//    pointNode = new Point(pointID, descriptor)
//    pointNode
//  }

}


object InternalNode {

  /**
    * Searches the index for the cluster leaders that is closest to the
    * given input point and returns the nearest b points as found by
    * comparing distances.
    *
    * @param current The current InternalNode of the index which can be
    *                obtained from the method buildIndexTree.
    * @param previous The previous InternalNode at the level before the current
    *                 level.
    * @param point Point used as reference in the search.
    * @param b The number of nearest clusters to return for extended searches.
    * @note The search does not always return the optimal result.
    * @return The nearest cluster leader of the index leafs to the
    *         input point.
    */
  def searchTheIndex(current: InternalNode, previous: InternalNode)(point: Point, b: Int): Array[InternalNode] = {

    if (current.isLeaf){
      previous.children.map(in =>
        (in, in.pointNode.eucDist(point))).sortBy(_._2).map(_._1).slice(0, b)
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
    * Finds the children of the given parent nodes with respect to the point `point`.
    *
    * @param parents Parent nodes.
    * @param point The point.
    * @return the children.
    */
  def findChildren(parents: Array[(InternalNode, Array[Long])])(point: Point): InternalNode = {
    val search = parents.collect{
      case x if x._2.contains(point.pointID) => x._1
    }
    InternalNode(search.asInstanceOf[Array[InternalNode]], point)
  }


  /**
    * Find the expected size of the index at level L.
    * Only used for easy logging of the index size.
    *
    * @param inputSize Number of points in the static input.
    * @param level The level of interest in the index. One is the leaf level.
    * @param L The depth of the index not including the root.
    * @return The expected size of the index at level 'level'.
    */
  def expectedIndexSize(inputSize: Long, level: Int, L: Int): Int = {
    // Set some constants used in the eCP approach
    val balancingFactor = 0        // Pick an additional balancingFactor % leafs
    val IOGranularity = 128 * 1024 // 128 KB IO granularity on a Linux OS
    val descriptorSize = 128 + 8   // Dimension of 128 with a Long descriptor - see ClusterInputFormat

    // Determine the leaderCount based on the CP approach (for debugging) or
    // the eCP approach (for the serious experiments).
    val leaderCounteCP = ceil(inputSize / floor(IOGranularity / descriptorSize)).toInt
    val leaderCountCP = ceil(pow(inputSize, 1 - level.toDouble / (L.toDouble + 1))).toInt

    if (level == 1) leaderCounteCP else leaderCountCP
  }


  /**
    * Finds the actual size of the index at level L.
    * Only used for easy logging of the index size.
    *
    * @param root The InternalNode designated as the root.
    * @param level The level of interest in the index. One is the leaf level.
    * @param L The depth of the index not including the root.
    * @return The actual size of the index at level 'level'.
    */
  def actualIndexSize(root: InternalNode, level: Int, L: Int): Int = {
    indexRecursion(root, level, L).distinct.length
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
    indexRecursion(root, 1, L).groupBy(_.pointID).mapValues(_.length)
  }

  /**
    * Finds the leafs of the InternalNode.
    *
    * @param root The InternalNode designated as the root.
    * @param L The depth of the index not including the root.
    * @return the leafs of the given InternalNode.
    */
  def getLeafs(root: InternalNode, L: Int): Array[Point] = {
    indexRecursion(root, 1, L).distinct
  }

  /**
    * Traverses the index and returns the set of Points at the
    * level `level`. The number of duplicate Points returned
    * represents the number of possible ways the Point can be
    * reached in a index search.
    *
    * @param root The InternalNode designated as the root.
    * @param level The level of interest in the index. One is the leaf level.
    * @param L The depth of the index not including the root.
    * @return All the Points at level `level` including duplicate entries
    *         representing the number of distinct paths that can be taken
    *         to reach the given Point.
    */
  private def indexRecursion(root: InternalNode, level: Int, L: Int): Array[Point] = {
    val depth = (L + 1) - level

    if (depth == 1)
      root.children.map(_.pointNode)
    else
      root.children.flatMap(indexRecursion(_, level + 1, L))
  }

}

