package project.distributed.container


case class Point(pointID: Long, descriptor: Vector[Float]) extends Serializable {

  def eucDist(that: Point): Double =
    Point.eucDist(this, that)


}

object Point{
  def eucDist(p1: Point, p2: Point): Double =
    scala.math.sqrt((p1.descriptor zip p2.descriptor).map { case (x, y) => scala.math.pow(y - x, 2.0) }.sum)



}
