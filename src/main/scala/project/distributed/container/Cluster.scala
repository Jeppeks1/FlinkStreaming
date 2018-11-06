package project.distributed.container

import java.io._

class Cluster(points: Vector[Point]) extends Serializable {

  // Define the member variables
//  private var knn: Vector[Point] = Vector[Point]()    // The k-nearest neighbors
//  private var queryPoint: Point = _     // The reference point for the kNN algorithm
//  private var maxDistance: Double = Double.MaxValue
//  private var maxIndex: Int = 0
//  private var k: Int = 3                // The parameter k for the k-nearest neighbors
//  private var n: Int = _                // Not a clue what this is yet


  def getPoints: Vector[Point] = points

  /*
  def add(point: Point): Cluster = {
    val distance = point.eucDist(this.queryPoint).distance

    if (distance < maxDistance){
      // The point should be added to the kNN vector
      if (knn.size < k - 1){
        // There is enough space in the kNN vector
        knn = knn :+ point
        maxIndex = maxIndex + 1
        maxDistance = point.distance
      }
      else if (knn.size < k){
        // Last empty space in the kNN vector
        knn = knn :+ point
        maxIndex = maxIndex + 1
        maxDistance = point.distance
      }
      else {
        // The kNN vector is full. Find which point to throw out.
        val idx = knn.indices.maxBy(knn)
        knn = knn.updated(idx, point)
      }
    }
    this
  }
*/

}

object Cluster {

  def staticDistance(queryPoint: Point, p: Point): Double = {
    p.eucDist(queryPoint)
  }
  
  def kNearestNeighbor(points: Vector[Point], queryPoint: Point, k: Int): Vector[(Point, Double)] = {
    points.map(p => (p, p.eucDist(queryPoint))).sortBy(_._2).slice(0, k)
  }

}



