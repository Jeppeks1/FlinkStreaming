package reader

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path

import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.language.implicitConversions

class TruthInputSpec extends FlatSpec with Checkers with Matchers {

  private val env = ExecutionEnvironment.getExecutionEnvironment
  private val par = 4

  // val siftPath = "hdfs://h1.itu.dk:8020/user/jeks/data/" + sift
  val siftPath1 = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftlarge\\"
  val siftPath2 = "file:\\C:\\Users\\Jeppe-Pc\\Documents\\Universitet\\IntelliJ\\Flink\\data\\siftmedium\\"
  val path1 = new Path(siftPath1 + "/truth/idx_1M.ivecs")
  val path2 = new Path(siftPath2 + "groundTruth.ivecs")

  val truth1: DataSet[(Int, Array[Int])] = env.createInput(new TruthInputFormat(path1)).setParallelism(par)
  val collectedTruth1: Vector[(Int, Array[Int])] = truth1.collect.toVector
  val sortedTruth1: Vector[Array[Int]] = collectedTruth1.sortBy(_._1).map(_._2)

  val truth2: DataSet[(Int, Array[Int])] = env.createInput(new TruthInputFormat(path2)).setParallelism(par)
  val collectedTruth2: Vector[(Int, Array[Int])] = truth2.collect.toVector
  val sortedTruth2: Vector[Array[Int]] = collectedTruth2.sortBy(_._1).map(_._2)

  behavior of "TruthInputFormat - siftlarge/idx_1M.ivecs"
  it should "read a monotically increasing series of truth vectors" in check {
    val zipped = collectedTruth1.sortBy(_._1).map(_._1).zipWithIndex
    zipped.forall(in => in._1 == in._2)
  }

  it should "contain unique index values" in check {
    collectedTruth1.map(_._1).length == collectedTruth1.map(_._1).distinct.length
  }

  it should "have the same size as the number of query points" in check {
    collectedTruth1.length == 100 || collectedTruth1.length == 10000
  }

  it should "have arrays that are of length k == 100 or k == 1000" in check {
    collectedTruth1.forall(in => in._2.length == 100 || in._2.length == 1000 )
  }

  it should "contain the correct values on a sample" in check {
    val truth0 = sortedTruth1(0)
    val truth345 = sortedTruth1(345)
    val truth2986 = sortedTruth1(2986)
    val truth8654 = sortedTruth1(8654)

    truth0.head == 504814 && truth345.head == 15283 &&
      truth2986.head == 631423 && truth8654.head == 158782
  }


  // -------------------------------------------------

  behavior of "TruthInputFormat - siftmedium/groundTruth.ivecs"
  it should "read a monotically increasing series of truth vectors" in check {
    val zipped = collectedTruth2.sortBy(_._1).map(_._1).zipWithIndex
    zipped.forall(in => in._1 == in._2)
  }

  it should "contain unique index values" in check {
    collectedTruth2.map(_._1).length == collectedTruth2.map(_._1).distinct.length
  }

  it should "have the same size as the number of query points" in check {
    collectedTruth2.length == 100 || collectedTruth2.length == 10000
  }

  it should "have arrays that are of length k == 100 or k == 1000" in check {
    collectedTruth2.forall(in => in._2.length == 100 || in._2.length == 1000 )
  }

  it should "contain the correct values on a sample" in check {
    val truth0 = sortedTruth2(0)
    val truth345 = sortedTruth2(345)
    val truth2986 = sortedTruth2(2986)
    val truth8654 = sortedTruth2(8654)

    truth0.head == 932085 && truth345.head == 16637 &&
      truth2986.head == 350270 && truth8654.head == 322504
  }

}
