package project.local.container

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._
import org.scalacheck.Arbitrary
import language.implicitConversions

import project.local.container.InternalNode._

class IndexTreeSpec extends FlatSpec with Checkers with Matchers {

  val idxTree = new IndexTree


  def descriptorGen(gen: Gen[Float]): Gen[Vector[Float]] =
    Gen.listOfN(128, gen).map(_.toVector)

  def pointsGen(gen: Gen[Float]): Gen[Vector[Point]] =
    Gen.nonEmptyListOf(descriptorGen(gen)).map(_.toVector.map(Point(0, _)))

  implicit def arbPoints: Arbitrary[Vector[Point]] =
    Arbitrary[Vector[Point]](pointsGen(arbitrary[Float]))


  behavior of "pickRandomLeafs"
  it should "only pick elements in the original input" in check {
    forAll { (points: Vector[Point], seed: Long) =>
      pickRandomLeafs(points, seed).forall(in => points.contains(in.clusterLeader))
    }
  }

  it should "pick unique elements" in check {
    forAll { (points: Vector[Point], seed: Long) =>
      val res = pickRandomLeafs(points, seed)
      res.distinct.size == res.size

    }
  }



  //  behavior of "buildIndexTree"
  //  it should "Any cluster leader should reside in its own cluster"

  // "The number of nodes on the next level should be strictly increasing"
  // "The clusterLeader at any given node should be contained in all the children"
  // Ehh, or something like that.

}
