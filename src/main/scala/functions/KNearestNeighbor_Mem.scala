package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.core.fs.Path

import org.slf4j.{Logger, LoggerFactory}

import container.InternalNode.searchTheIndex
import container.{InternalNode, Point}

/**
  * Determines the K-Nearest neighbors for each incoming point. The input is
  * (queryPoint, Time, Vector[InternalNode]) which contains the clusterIDs to be
  * searched through. The result is (queryPointID, Vector[(pointID, distance)])
  * which is a vector of distances from the queryPointID to the pointID.
  *
  * This class is used for the memory-based approach.
  *
  * @param clusteredPoints An array containing the entire clustered points dataset.
  * @param root            The root node of the index.
  * @param leafs           Array of Points containing the cluster leaders at the buttom level of the index.
  * @param clusterPath     The base path to the directory where the clustered points are written.
  * @param b               The number of nearest clusters to search through for each query point.
  * @param k               The parameter determining the number of nearest neighbors to return.
  */
final class KNearestNeighbor_Mem(clusteredPoints: Array[(Point, Long)],
                                 root: InternalNode,
                                 leafs: Array[Point],
                                 clusterPath: Path,
                                 b: Int,
                                 k: Int) extends RichMapFunction[Point, (Long, Long, Array[(Long, Double)])] {

  private val log: Logger = LoggerFactory.getLogger(classOf[KNearestNeighbor_Mem])

  override def map(qp: Point): (Long, Long, Array[(Long, Double)]) = {
    // Latency metric
    val time = System.currentTimeMillis()

    // Determine the b closest clusters to the query point based on the distance to the cluster leader
    val clusterIDs = if (root != null) {
      // Use the index to determine the nearest clusterIDs
      searchTheIndex(root, null)(qp, b)
    } else {
      // Scan the leafs to determine the nearest clusterIDs
      leafs
        .map { p => (p.pointID, p.eucDist(qp)) }
        .sortBy(_._2)
        .map(_._1)
        .distinct
        .slice(0, b)
    }

    // This is used in workaround 1, where the clusteredPoints are passed as a parameter.
    // withFilter applies the map only to elements that satisfies the predicate.
    val knn = clusteredPoints
      .withFilter(cp => clusterIDs.contains(cp._2))
      .map(cp => (cp._1.pointID, qp.eucDist(cp._1)))
      .distinct
      .sortBy(_._2)
      .slice(0, k)

    (qp.pointID, time, knn)
  }

}