package project.distributed.container

import project.distributed.container.Cluster
import org.apache.flink.api.scala._

/**
  * The basic abstraction for a point to point comparison of the query point
  * to the point with the given pointID in the k-nearest neighbor method.
  *
  * @param pointID    The unique ID of this point.
  * @param descriptor The actual point to be compared against the query point.
  */
case class Point(pointID: Long,
                 descriptor: Vector[Float]) extends Serializable {


  var distance: Double = Double.MaxValue

  def eucDist(that: Point): Point = {
    this.distance = Point.eucDist(this, that)
    this
  }


  def toCluster: Cluster = {
    val points = ExecutionEnvironment.getExecutionEnvironment.fromElements(Point(-1, Vector()))
    new Cluster(points, this)
  }

}

object Point {

  def eucDist(p1: Point, p2: Point): Double =
    scala.math.sqrt((p1.descriptor zip p2.descriptor).map { case (x, y) => scala.math.pow(y - x, 2.0) }.sum)

  implicit def orderByDistance[A <: Point]: Ordering[A] =
    Ordering.by(_.distance)

}
