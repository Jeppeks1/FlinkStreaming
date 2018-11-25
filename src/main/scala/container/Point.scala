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
  def this() {
    this(-1, Array[Float]())
  }

  override def toString: String = "ID = " + this.pointID + ";" + descriptor.toVector.toString

  /**
    * Some methods may return an Array with duplicate Points and call distinct
    * on the array, so the equals method must be handled to consider the content
    * of the class and not the reference.
    *
    * Update: This does work as intended, but all the calls to the .distinct method
    * on both the Array[Point] and DataSet[Point] uses a different method to determine
    * duplicate elements.
    *
    * @param obj The object to compare with.
    * @return true if the objects are equal otherwise false.
    */
  override def equals(obj: scala.Any): Boolean = {

    // Check if compared to itself
    if (super.equals(obj)) return true

    // Check if the object is an instance of a Point
    if (!obj.isInstanceOf[Point]) return false

    // Get the object as a Point and compare the contents
    val castPoint = obj.asInstanceOf[Point]
    if ((this.pointID == castPoint.pointID) && (this.descriptor sameElements castPoint.descriptor)) true else false
  }

  // Convenience method to call the optimized distance.
  def eucDist(that: Point): Double = {
    Point.optimizedDist(this, that)
  }

  // Points are serialized to a file using this method.
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

  // Serialized points are read from a file using this method.
  override def read(in: DataInputView): Unit = {
    // Read the serialized pointID
    pointID = in.readLong

    // Read the chars back into a float
    var dimCount = 0
    var arr = Array[Float]()
    while (dimCount < 128) {
      val float = in.readChar().toFloat
      arr = arr :+ float
      dimCount = dimCount + 1
    }

    // Set the descriptor
    descriptor = arr
  }
}

object Point {

  def optimizedDist(p1: Point, p2: Point): Double = {
    var dist: Double = 0

    for (x <- p2.descriptor.indices) {
      val a = p1.descriptor(x) - p2.descriptor(x)
      dist = dist + a * a
    }

    dist
  }

}
