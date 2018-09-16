package project.local.container

import java.io._

import org.apache.commons.io.output.CountingOutputStream

class Cluster(var points: Vector[Point],
              var clusterID: Long) extends Serializable {

  // Define the member variables
  private var knn: Vector[Point] = _ // The k-nearest neighbors
  private var queryPoint: Point = _     // The reference point for the kNN algorithm
  private var maxDistance: Int = _
  private var maxIndex: Int = _
  private var k: Int = _                // The parameter k for the k-nearest neighbors
  private var n: Int = _                // Not a clue what this is yet


  /**
    * Convenience method to set a reference point for the distance
    * calculations in the subsequent k-nearest neighbor algorithm.
    * @param p The query point
    */
  private def setQueryPoint(p: Point): Unit =
    this.queryPoint = p


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


  def kNearestNeighbor(queryPoint: Point, k: Int): Vector[Point] = {
    setQueryPoint(queryPoint)

    points.map(distance).sorted.slice(0, k)
  }


}

object Cluster {

  def writeClusters(clusters: Vector[Cluster],
                    filename: String): Map[Long, Long] = {

    val file = new File(filename)
    val fileOutputStream = new FileOutputStream(file)
    val objectOutputStream = new ObjectOutputStream(fileOutputStream)
    val countingOutputStream = new CountingOutputStream(objectOutputStream)

    var map = Map[Long, Long]()
    for (cluster <- clusters){
      map = map + (cluster.clusterID -> file.length)
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



}



