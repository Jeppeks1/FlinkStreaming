package container

import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.core.io.IOReadableWritable

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

  override def toString: String = "ID = " + this.pointID + ";" + descriptor.toVector.toString
//      override def toString: String = "Point(ID = " + this.pointID + ")" // For debugging

  def eucDist(that: Point): Double = {
    Point.optimizedDist(this, that)
  }

  // Points are serialized in ClusterOutputFormat via. this method.
  override def write(out: DataOutputView): Unit = {
    // Write the pointID
    out.writeLong(pointID)

    // The floats from the input do not fit in one byte which
    // is what the DataOuputView mostly expects. We can use a
    // hack, which converts the floats to chars and then to
    // a string, which can be written and then de-converted in
    // the read method.
    out.writeChars(descriptor.map(_.toChar).mkString)
  }

  // This read method has not been tested, but it is not used anywhere,
  // as it seems to be usable only within the write() method of DataSets.
  override def read(in: DataInputView): Unit = {
    // Read the serialized pointID
    pointID = in.readLong

    // Read the chars into a string
    val string = in.readLine()

    // Make sure something was read
    assert(string.length > 0)

    // Convert the chars to floats
    descriptor = string.toCharArray.map(_.toFloat)
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
