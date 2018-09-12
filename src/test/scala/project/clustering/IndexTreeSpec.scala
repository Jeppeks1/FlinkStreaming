package project.clustering

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.Checkers
import org.scalacheck._
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary._
import org.scalacheck.Arbitrary
import language.implicitConversions

import project.container.SiftDescriptor


class IndexTreeSpec extends FlatSpec with Checkers with Matchers {

  val idxTree = new IndexTree[Int]
  val n = 10

  def vectorOfN[A](n: Int, gen: Gen[A]): Gen[Vector[A]] =
    Gen.listOfN(n, gen).map(la => la.foldLeft(Vector[A]())(_ :+ _))

  def siftDescriptorOfN[A](n: Int, gen: Gen[A]): Gen[SiftDescriptor[A]] =
    vectorOfN(n, gen).map(vec => SiftDescriptor(1, vec))

  def pointsOfN[A](n: Int, gen: Gen[A]): Gen[Vector[SiftDescriptor[A]]] =
    vectorOfN(n, siftDescriptorOfN(n, gen))

  implicit def arbPointsOfN[A](implicit arb: Arbitrary[A]): Arbitrary[Vector[SiftDescriptor[A]]] =
    Arbitrary[Vector[SiftDescriptor[A]]](pointsOfN(n, arbitrary[A]))


  behavior of "pickRandomLeaders"
  it should "only pick elements in the original input" in check {
    forAll { (points: Vector[SiftDescriptor[Int]], seed: Long) =>
      forAll(Gen.choose(0, points.size)) { n =>
        idxTree.pickRandomLeaders(points, n, seed).forall(SD => points.contains(SD))
      }
    }
  }

  it should "pick unique elements" in check {
    forAll { (points: Vector[SiftDescriptor[Int]], seed: Long) =>
      forAll(Gen.choose(0, points.size)) { n =>
        val res = idxTree.pickRandomLeaders(points, n, seed)
        res.distinct.size == res.size
      }
    }
  }

  it should "produce an output of size n" in check {
    forAll { (points: Vector[SiftDescriptor[Int]], seed: Long) =>
      forAll(Gen.choose(0, points.size)) { n =>
        idxTree.pickRandomLeaders(points, n, seed).size == n
      }
    }
  }

  behavior of "buildIndexTree"
  it should "Any cluster leader should reside in its own cluster"



}
