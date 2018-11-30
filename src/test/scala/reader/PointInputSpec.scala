package reader

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.language.implicitConversions

import org.apache.flink.api.common.operators.Order
import container.Point

class PointInputSpec extends FlatSpec with Checkers with Matchers {

  private val env = ExecutionEnvironment.getExecutionEnvironment
  private val par = 4

  val siftPath = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftlarge\\"
//  val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/siftlarge"
  val path = new Path(siftPath + "/query.bvecs")

  val points = env.createInput(new PointInputFormat(path, 1))
    .name("Point Source")
    .setParallelism(par)
    .collect
    .toVector

  val test = points.filter(_.pointID == 8909).head

  behavior of "PointInputFormat - query.bvecs"
  it should "read a monotically increasing series of Points" in check {
    points.map(_.pointID).sorted.zipWithIndex.forall(in => in._1 == in._2)
  }

  it should "contain unique index values" in check {
    points.map(_.pointID).size == points.map(_.pointID).distinct.size
  }

  it should "not contain any negative elements" in check {
    points.forall(in => in.descriptor.forall(_ >= 0))
  }

  it should "pass the check on a specific sample" in check {
    val qp6 = points.filter(_.pointID == 6).head
    val qp5863 = points.filter(_.pointID == 5863).head
    val qp8907 = points.filter(_.pointID == 8907).head

    qp5863.descriptor.head == 132 && qp5863.descriptor(8) == 132 &&
      qp8907.descriptor(8) == 166 && qp6.descriptor(8) == 187
  }




}
