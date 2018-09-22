package project.distributed

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.common.operators.Order
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

import project.distributed.reader.FeatureVector._
import project.distributed.container.InternalNode
import project.distributed.container.IndexTree._
import project.distributed.container.Point

import scala.collection.JavaConverters._

import java.security.InvalidParameterException
import java.lang.Iterable

object DeCP {

  /**
    * Usage:
    * {{{
    *   DeCP --queryPoints <path>
    *        --featureVectors <path>
    *        --groundTruth <path>
    *        --method <String>
    *        --L <Int>
    *        --a <Int>
    *        --b <Int>
    *        --k <Int>
    * }}}
    */
  def main(args: Array[String]): Unit = {

    // Initialize input variables
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val featureVectorPath = params.get("featureVectors")
    val groundTruthPath = params.get("groundTruths")
    val queryPointPath = params.get("queryPoints")
    val method = params.get("method")
    val k = params.get("k").toInt
    val a = params.get("a").toInt

    // Get the execution environment and read the data
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val groundTruth: DataSet[Vector[Int]] = ivecs_truth(env, groundTruthPath)
    val queryPoints: DataSet[Point] = readFeatureVector(env, queryPointPath)
    val points: DataSet[Point] = readFeatureVector(env, featureVectorPath)
    val root: InternalNode = buildIndexTree(points, params)

    // The set of all points grouped on the cluster they belong to.
    // Will later be used to retrieve the relevant set of clusters
    // and perform a kNN search within them.
    val clusteredPoints = points
      .map(p => (p, searchTheIndex(root, null)(p, a)))
      .flatMap(new FlatMapper)
      .groupBy(1)

    // Process the query points
    val knn = if (method == "scan") {
      // Perform a full sequential scan
      clusteredPoints
        .reduceGroup(new SequentialScan)
        .withBroadcastSet(queryPoints, "queryPoints")
        .groupBy(0)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new kNearestNeighbors(k))
        .setParallelism(1)
        .sortPartition(0, Order.ASCENDING)

    }
    else if (method == "index") {
      // Discover which clusters to search through.
      val query2cluster = queryPoints
        .map(qp => (qp, searchIndex(root)(qp).pointID))
      //TODO: Introduce the b parameter - query2clusters with an s in the end.

      // Search through the relevant clusters.
      clusteredPoints
        .reduceGroup(new FindDistances)
        .withBroadcastSet(query2cluster, "query2cluster")
        .groupBy(0)
        .sortGroup(2, Order.ASCENDING)
        .reduceGroup(new kNearestNeighbors(k))
        .setParallelism(1) // Sort globally
        .sortPartition(0, Order.ASCENDING)
    }
    else throw new InvalidParameterException("Invalid or missing input parameter --method. " +
      "See documentation for valid options.")

    // Compare the result to the ground truth
    val compared = knn
      .map(new compareToGroundTruth(k))
      .withBroadcastSet(groundTruth, "groundTruth")

    compared.print


  }


  /**
    * Flattens the incoming (Point, Vector[InternalNode]) records and retrieves the clusterID
    * from each InternalNode, to be used for grouping. The resulting type is (Point, clusterID).
    */
  final class FlatMapper extends FlatMapFunction[(Point, Vector[InternalNode]), (Point, Long)] {

    def flatMap(input: (Point, Vector[InternalNode]),
                out: Collector[(Point, Long)]): Unit = {
      input._2.foreach{ in =>
        out.collect((input._1, in.clusterLeader.pointID))
      }
    }
  }


  /**
    * The SequentialScan class takes the clustered input points on the format (Point, clusterID)
    * and reduces all the points into a (queryPointID, pointID, distance) record. This process is
    * repeated O(n * qp) times constituting a full sequential scan.
    *
    * @note The clusterID is not used in this class and the original dataset of points could have
    *       been used instead. As the clustered points are written to disk during pre-processing,
    *       it is faster to simply read that file, rather than parsing the entire input again.
    */
  final class SequentialScan extends RichGroupReduceFunction[(Point, Long), (Long, Long, Double)] {
    private var queryPoints: Traversable[Point] = _

    override def open(parameters: Configuration): Unit =
      queryPoints = getRuntimeContext.getBroadcastVariable[Point]("queryPoints").asScala

    def reduce(it: Iterable[(Point, Long)],
               out: Collector[(Long, Long, Double)]): Unit = {
      it.iterator().asScala.foreach { pl => // For each (Point, clusterID) in the incoming group
        queryPoints.foreach { qp => // For each query point
          out.collect(qp.pointID, pl._1.pointID, pl._1.eucDist(qp).distance)
        }
      }
    }
  }


  /**
    * The FindDistances class takes the clustered input points on the format (Point, clusterID)
    * and reduces the input to a (queryPointID, pointID, distance) record. The query2cluster
    * variable contains a mapping from each query point to the cluster leader it is closest to.
    * This mapping is used to filter irrelevant clusters and compute the relevant distances
    * in one go.
    */
  final class FindDistances extends RichGroupReduceFunction[(Point, Long), (Long, Long, Double)] {
    private var query2cluster: Traversable[(Point, Long)] = _

    override def open(parameters: Configuration): Unit =
      query2cluster = getRuntimeContext.getBroadcastVariable[(Point, Long)]("query2cluster").asScala

    def reduce(it: Iterable[(Point, Long)],
               out: Collector[(Long, Long, Double)]): Unit = {
      it.iterator().asScala.foreach(pl =>
        query2cluster.foreach { ql =>
          if (ql._2 == pl._2) {
            val point = pl._1.eucDist(ql._1)
            out.collect((ql._1.pointID, point.pointID, point.distance))
          }
        })
    }
  }

  /**
    * Determines the k-nearest neighbors for each incoming (queryPointID, pointID, distance)
    * record. The resulting output is a record (queryPointID, List[pointID]) containing the
    * query point ID, a list of the nearest points of size k and the distance from the query
    * point to the nearest point in the scanned cluster.
    *
    * @param k The number of nearest neighbors to return.
    * @note There is no correctness guarantee for the k-nearest neighbor algorithm and the
    *       size of the returned list of nearest neighbors may be smaller than k.
    */
  final class kNearestNeighbors(k: Int) extends RichGroupReduceFunction[(Long, Long, Double), (Long, List[Long])] {

    def reduce(it: Iterable[(Long, Long, Double)],
               out: Collector[(Long, List[Long])]): Unit = {
      val group = it.iterator().asScala.toList
      out.collect((group.head._1, group.map(_._2).take(k)))
    }
  }


  /**
    * Compares a result to the ground truth feature vector in the most convoluted way possible.
    *
    * @param k The number of nearest neighbors in the incoming result set.
    * @note The actual number of returned nearest neighbor might be smaller than k, which is why
    *       the size of the zipped result/truth vector is used.
    */
  final class compareToGroundTruth(k: Int) extends RichMapFunction[(Long, List[Long]), (Long, Double)] {
    private var groundTruth: Traversable[Vector[Int]] = _

    override def open(parameters: Configuration): Unit =
      groundTruth = getRuntimeContext.getBroadcastVariable[Vector[Int]]("groundTruth").asScala

    def map(in: (Long, List[Long])): (Long, Double) = {
      val vecVec = groundTruth.toVector
      val vec = vecVec(in._1.toInt).take(k)
      val compare = in._2.take(k).zip(vec)
      val size = compare.size
      (in._1, compare.map(li => if (li._1 == li._2.toInt) 1.0 / size else 0.0).sum)
    }
  }


}
