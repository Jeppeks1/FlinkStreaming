import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, _}
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

import scala.language.implicitConversions


class BatchDeCPSpec extends FlatSpec with Checkers with Matchers {

  import container.{InternalNode, Point}


  def descriptorGen(gen: Gen[Float]): Gen[Array[Float]] =
    Gen.listOfN(128, gen).map(_.toArray)

  def pointsGen(gen: Gen[Float]): Gen[Array[Point]] =
    Gen.nonEmptyListOf(descriptorGen(gen)).map(_.toArray.map(new Point(0, _)))

  implicit def arbPoints: Arbitrary[Array[Point]] =
    Arbitrary[Array[Point]](pointsGen(arbitrary[Float]))

//
//  val fakeRoot = InternalNode(Array(
//    InternalNode(Array(
//      InternalNode(Array(
//        InternalNode(Array(), new Point(0, Array())),
//        InternalNode(Array(), new Point(1, Array()))
//      ), new Point(1, Array())), // Level 2
//      InternalNode(Array(
//        InternalNode(Array(), new Point(1, Array())),
//        InternalNode(Array(), new Point(2, Array()))
//      ), new Point(2, Array())) // Level 2
//    ), new Point(1, Array())), // Level 3
//
//    InternalNode(Array(
//      InternalNode(Array(
//        InternalNode(Array(), new Point(1, Array())),
//        InternalNode(Array(), new Point(2, Array()))
//      ), new Point(2, Array())), // Level 2
//      InternalNode(Array(
//        InternalNode(Array(), new Point(2, Array())),
//        InternalNode(Array(), new Point(3, Array())),
//        InternalNode(Array(), new Point(4, Array()))
//      ), new Point(4, Array())), // Level 2
//      InternalNode(Array(
//        InternalNode(Array(), new Point(4, Array())),
//        InternalNode(Array(), new Point(5, Array()))
//      ), new Point(5, Array())) // Level 2
//    ), new Point(4, Array())), // Level 3
//
//    InternalNode(Array(
//      InternalNode(Array(
//        InternalNode(Array(), new Point(4, Array())),
//        InternalNode(Array(), new Point(5, Array()))
//      ), new Point(5, Array())), // Level 2
//      InternalNode(Array(
//        InternalNode(Array(), new Point(5, Array())),
//        InternalNode(Array(), new Point(6, Array())),
//        InternalNode(Array(), new Point(7, Array()))
//      ), new Point(7, Array())) // Level 2
//    ), new Point(7, Array())) // Level 3
//  ), new Point(1, Array())) // Level 4


  // Perform tests on the reader package and test these:
  //  val testPoints = points.collect.toVector // points is the DataSet
  //  val test = readFeatureVector(featureVectorPath)
  //  val test1 = testPoints.size == test.size
  //  val test2 = testPoints.map(_.pointID).distinct.size == testPoints.size
  //  val test3 = testPoints.forall(p => test.contains(p))

  //  behavior of "pickRandomLeafs"
  //  it should "only pick elements in the original input" in check {
  //    forAll { (points: Vector[Point], seed: Long) =>
  //      pickRandomLeafs(points, seed).forall(in => points.contains(in.clusterLeader))
  //    }
  //  }
  //
  //  it should "pick unique elements" in check {
  //    forAll { (points: Vector[Point], seed: Long) =>
  //      val res = pickRandomLeafs(points, seed)
  //      res.distinct.size == res.size
  //
  //    }
  //  }


  //  behavior of "buildIndexTree"
  //  it should "Any cluster leader should reside in its own cluster"

  // "The number of nodes on the next level should be strictly increasing"
  // "The clusterLeader at any given node should be contained in all the children"
  // Ehh, or something like that.

}
