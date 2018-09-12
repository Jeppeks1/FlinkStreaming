package project.distributed.container


abstract class Point {
  def id: Long
  def descriptor: Vector[_]
}

case class FloatingPoint(id: Long, descriptor: Vector[Float]) extends Point
case class IntegerPoint(id: Long, descriptor: Vector[Int]) extends Point
