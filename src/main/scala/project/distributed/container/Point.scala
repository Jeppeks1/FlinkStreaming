package project.distributed.container

import org.apache.flink.api.scala._

/**
  * The basic abstraction for a point to point comparison of the query point
  * to the point with the given pointID in the k-nearest neighbor method.
  *
  * @param pointID    The unique ID of this point.
  * @param descriptor The actual point to be compared against the query point.
  */
case class Point(var pointID: Long,
                 var descriptor: Vector[Float]) extends Serializable {

  override def toString: String = "Point(ID = " + this.pointID + ")"

  def eucDist(that: Point): Double = {
    Point.optimizedDist(this, that)
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
