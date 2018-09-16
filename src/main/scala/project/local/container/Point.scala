package project.local.container

/**
  * The basic abstraction for a point to point comparison of the query point
  * to the point with the given pointID in the k-nearest neighbor method.
  *
  * @param pointID    The unique ID of this point.
  * @param descriptor The actual point to be compared against the query point.
  */
case class Point(pointID: Long,
                 descriptor: Vector[Float]) extends Serializable
                                            with Comparable[Point] {


  var distance: Double = Double.MaxValue

  // Overriding the compareTo method defines the ordering of Points
  override def compareTo(p: Point): Int = {
    if (p.distance > this.distance) -1
    else if (p.distance < this.distance) 1
    else 0
  }


  def eucDist(that: Point): Point = {
    this.distance = Point.eucDist(this, that)
    this
  }

}

object Point {

  def eucDist(p1: Point, p2: Point): Double =
    scala.math.sqrt((p1.descriptor zip p2.descriptor).map { case (x, y) => scala.math.pow(y - x, 2.0) }.sum)


}
