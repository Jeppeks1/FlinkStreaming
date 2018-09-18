package project.distributed.container

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import java.io._

import org.apache.commons.io.output.CountingOutputStream

class Cluster(var points: DataSet[Point],
              var clusterLeader: Point) extends Serializable {

  // Define the member variables
  private var knn: Vector[Point] = Vector[Point]()    // The k-nearest neighbors
  private var queryPoint: Point = _     // The reference point for the kNN algorithm
  private var maxDistance: Double = Double.MaxValue
  private var maxIndex: Int = 0
  private var k: Int = 3                // The parameter k for the k-nearest neighbors
  private var n: Int = _                // Not a clue what this is yet


  /**
    * Convenience method to set a reference point for the distance
    * calculations in the subsequent k-nearest neighbor algorithm.
    * @param p The query point
    */
  private def setQueryPoint(p: Point): Cluster = {
    this.queryPoint = p
    this
  }

  /**
    * Calculates the distance between the query point and the point p
    * and updates the distance member in the point p.
    * The method setQueryPoint must be called beforehand.
    * @param p Input point
    * @return The input point p updated with the distance
    *         between the query point and p
    */
  def distance(p: Point): Point = {
    p.eucDist(queryPoint)
  }

  def getKnnVector: Vector[Point] =
    knn


  def kNearestNeighbor(queryPoint: Point, k: Int): DataSet[Point] = {
    setQueryPoint(queryPoint)

    this.points.map{p => Cluster.staticDistance(queryPoint, p)}
      .sortPartition(_.distance, Order.ASCENDING)
      .setParallelism(1) // Sorts the dataset in one partition, not across multiple workers
      .first(k)
  }


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


}

object Cluster {

  def staticDistance(queryPoint: Point, p: Point): Point = {
    p.eucDist(queryPoint)
  }


  /**
    * Convenience method to set a reference point for the distance
    * calculations in the subsequent k-nearest neighbor algorithm.
    * @param qp The query point
    */
  def setEmptyQueryPoint(qp: Point): Cluster =
    new Cluster(null, qp).setQueryPoint(qp)

  def setQueryPoint(c: Cluster, qp: Point): Cluster =
    c.setQueryPoint(qp)



  def kNearestNeighbor(pointsIn: DataSet[Point], queryPoint: Point, k: Int): DataSet[Point] = {
    val cluster = setEmptyQueryPoint(queryPoint)

    pointsIn.map(p => cluster.distance(p))
      .sortPartition(_.distance, Order.ASCENDING)
      .setParallelism(1) // Sorts the dataset in one partition, not across multiple workers
      .first(k)
  }




  /*
  def writeClusters(clusters: Vector[Cluster],
                    filename: String): Map[Long, Long] = {

    val file = new File(filename)
    val fileOutputStream = new FileOutputStream(file)
    val objectOutputStream = new ObjectOutputStream(fileOutputStream)
    val countingOutputStream = new CountingOutputStream(objectOutputStream)

    var map = Map[Long, Long]()
    for (cluster <- clusters){
      map = map + (cluster.clusterLeader.pointID -> file.length)
      objectOutputStream.writeObject(cluster)
    }

    objectOutputStream.flush()
    objectOutputStream.close()
    map
  }


  def readCluster(clusterID: Long, map: Map[Int, Long], filename: String): Vector[Cluster] = {
    import java.io.RandomAccessFile

    val randomAccessFile = new RandomAccessFile(filename, "r")

    val fileInputStream = new FileInputStream(filename)
    val objectInputStream = new ObjectInputStream(fileInputStream)
    objectInputStream.skipBytes(map(clusterID.toInt).toInt)

    var vec = Vector[Cluster]()
    var condition = true
    while(condition){
      if (fileInputStream.available() != 0) {
        vec = vec :+ objectInputStream.readObject().asInstanceOf[Cluster]
      }
      else
        condition = false
    }

    objectInputStream.close()

    vec
  }
  */


}



