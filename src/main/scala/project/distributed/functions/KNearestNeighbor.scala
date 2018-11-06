package project.distributed.functions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import project.distributed.StreamingDeCP
import project.distributed.container.{InternalNode, Point}

/**
  * Determines the K-Nearest neighbors for each incoming point. The input is
  * (queryPoint, Vector[InternalNode]) which contains the clusterIDs to be
  * searched through. The result is (queryPointID, Vector[(pointID, distance)])
  * which is a vector of distances from the queryPointID to the pointID.
  *
  * @param k The parameter determining the number of nearest neighbors to return.
  */
final class KNearestNeighbor(k: Int) extends RichFlatMapFunction[(Point, Vector[InternalNode]),
  (Long, Vector[(Long, Double)])] {

  private var clusteredPoints: Vector[(Point, Long)] = _

  override def open(parameters: Configuration): Unit = {
    clusteredPoints = StreamingDeCP.clusteredPoints
  }

  override def flatMap(input: (Point, Vector[InternalNode]),
                       out: Collector[(Long, Vector[(Long, Double)])]): Unit = {
    val qp = input._1
    val clusterIDs = input._2.map(_.pointNode.pointID).distinct

    // withFilter applies the map only to elements that satisfies the predicate
    val distances = clusteredPoints
      .withFilter(cp => clusterIDs.contains(cp._2))
      .map(cp => (cp._1.pointID, input._1.eucDist(cp._1)))
      .distinct

    val knn = distances.sortBy(_._2).slice(0, k)
    out.collect((qp.pointID, knn))
  }

}