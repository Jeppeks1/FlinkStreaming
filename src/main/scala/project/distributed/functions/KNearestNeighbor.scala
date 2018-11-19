package project.distributed.functions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.core.fs.Path
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import project.distributed.container.{InternalNode, Point}
import project.distributed.reader.ClusterInputFormat

/**
  * Determines the K-Nearest neighbors for each incoming point. The input is
  * (queryPoint, Time, Vector[InternalNode]) which contains the clusterIDs to be
  * searched through. The result is (queryPointID, Vector[(pointID, distance)])
  * which is a vector of distances from the queryPointID to the pointID.
  *
  * @param clusteredPoints An array containing the entire clustered points dataset
  * @param clusterPath The base path to the directory where the clustered points are written
  * @param k The parameter determining the number of nearest neighbors to return.
  */
final class KNearestNeighbor(clusteredPoints: Array[(Point, Long)], clusterPath: Path, k: Int)
extends RichFlatMapFunction[(Point, Long, Array[InternalNode]), (Long, Long, Array[(Long, Double)])] {

  protected val log: Logger = LoggerFactory.getLogger(classOf[KNearestNeighbor])

  override def flatMap(input: (Point, Long, Array[InternalNode]),
                       out: Collector[(Long, Long, Array[(Long, Double)])]): Unit = {
    val qp = input._1
    val clusterIDs = input._3.map(_.pointNode.pointID).distinct

    val knn = if (clusteredPoints != null){
      // This is used in workaround 1, where the clusteredPoints are passed as a parameter
      // withFilter applies the map only to elements that satisfies the predicate
      clusteredPoints
        .withFilter(cp => clusterIDs.contains(cp._2))
        .map(cp => (cp._1.pointID, input._1.eucDist(cp._1)))
        .distinct
        .sortBy(_._2)
        .slice(0, k)

    } else {
      // Workaround 2 reads the clustered points from a file named after the clusterID
      // Create a placeholder for the kNN, which will be updated after each iteration.
      var currentKNN = Array[(Long, Double)]()

      clusterIDs.foreach{ id =>
        // Define the path to read
        val path = new Path(clusterPath, "clusterID-" + id)

        // Read the clustered points and calculate the kNN within´the cluster
        val cpKNN = ClusterInputFormat.readCluster(path)
          .map(cp => (cp.pointID, qp.eucDist(cp)))
          .distinct
          .sortBy(_._2)
          .slice(0, k)

        // Combine the two arrays of kNNs into one
        if (currentKNN == null){
          currentKNN = cpKNN
        } else {
          currentKNN = (currentKNN ++ cpKNN)
            .distinct
            .sortBy(_._2)
            .slice(0, k)
        }
      }

      currentKNN
    }

    out.collect((qp.pointID, input._2, knn))
  }

}