package container

import org.apache.flink.core.io.IOReadableWritable
import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/**
  * The basic abstraction for a point to point comparison of the query point
  * to the point with the given pointID in the k-nearest neighbor method.
  *
  * @param pointID    The unique ID of this point.
  * @param descriptor The actual point to be compared against the query point.
  */
case class Point(var pointID: Long,
                 var descriptor: Array[Float]) extends IOReadableWritable {

  // Flink requires a default constructor to infer the POJO type and avoid GenericType
  def this(){
    this(-1, Array[Float]())
  }

//  override def toString: String = "ID = " + this.pointID + ";" + descriptor.toVector.toString
      override def toString: String = "Point(ID = " + this.pointID + ")" // For debugging

  def eucDist(that: Point): Double = {
    Point.optimizedDist(this, that)
  }

  override def write(out: DataOutputView): Unit = {
    out.writeLong(pointID)
    out.write(descriptor.map(_.toByte))
  }

  override def read(in: DataInputView): Unit = {
    // Read the serialized pointID
    pointID = in.readLong

    // Read the floats into a byte array
    val bytes = new Array[Byte](128)
    val bytesRead = in.read(bytes)

    // Make sure something was read
    assert(bytesRead > 0)

    // Convert the bytes to floats
    descriptor = bytes.map(_.toFloat)
  }

}

object Point {

  // This is really really inefficient
  def eucDist(p1: Point, p2: Point): Double =
    scala.math.sqrt((p1.descriptor zip p2.descriptor).map { case (x, y) => scala.math.pow(y - x, 2.0) }.sum)

  def optimizedDist(p1: Point, p2: Point): Double = {
    var dist: Double = 0

    for (x <- p2.descriptor.indices){
      var a = p1.descriptor(x) - p2.descriptor(x)
      dist = dist + a * a
    }

    dist
  }

}
