package project.container


case class SiftDescriptor[+A](ID: Int,
                              vector: Vector[A]) {


}

object SiftDescriptor {

//  def euclideanDistance(sd1: SiftDescriptor[Int], sd2: SiftDescriptor[Int]): Double = {
//    scala.math.sqrt((sd1.vector zip sd2.vector).map { case (x, y) => scala.math.pow(y - x, 2.0) }.sum)
//  }
//
//  def euclideanDistance(sd1: SiftDescriptor[Float], sd2: SiftDescriptor[Float]): Double = {
//    scala.math.sqrt((sd1.vector zip sd2.vector).map { case (x, y) => scala.math.pow(y - x, 2.0) }.sum)
//  }

}



