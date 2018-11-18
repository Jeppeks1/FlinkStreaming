package project.distributed.container

import org.apache.flink.core.io.IOReadableWritable
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

import scala.math.{ceil, pow}
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
      // Distances are sorted using .minBy
      val nearestChild = current.children.map(in =>
        (in, in.pointNode.eucDist(point))).minBy(_._2)._1

      searchTheIndex(nearestChild, current)(point, b)
    }
  }

}

