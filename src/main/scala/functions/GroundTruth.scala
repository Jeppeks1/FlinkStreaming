package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import scala.collection.JavaConverters._

/**
  * Takes a (queryPointID, time, Vector[(pointID, distance)]) and returns the results along with
  * a comparison to the ground truth. The output is a single record of (queryPointID, time, hit, recall, knn)
  * where the time value is a latency measure, the hit and recall variables are accuracy metrics and
  * knn is the k-Nearest neighbors of the queryPoint.
  */
final class GroundTruth(k: Int)
  extends RichMapFunction[(Long, Array[(Long, Double)]), (Long, Long, Double, Double, Vector[Long])] {

  private var groundTruthSorted: Array[Array[Int]] = _

  override def open(parameters: Configuration): Unit = {
    val groundTruth = getRuntimeContext.getBroadcastVariable[(Int, Array[Int])]("groundTruth").asScala
    groundTruthSorted = groundTruth.toArray.sortBy(_._1).map(_._2)
  }

  override def map(value: (Long, Array[(Long, Double)])): (Long, Long, Double, Double, Vector[Long]) = {
    // Latency metric
    val start = System.currentTimeMillis()

    // Get the query point from the input
    val queryPointID = value._1

    // Get the first k elements from the ground truth vector
    val truth = groundTruthSorted(queryPointID.toInt).slice(0, k)

    // There is no way to know in which order two points occur in the groundTruth,
    // if they have the same distance to the queryPoint, so we sort first by distance,
    // and then on the pointID in both directions.
    val knnRight = value._2.sortBy(in => (in._1, -in._1)).map(_._1)
    val knnLeft = value._2.sortBy(in => (in._1, in._1)).map(_._1)

    // Calculate the accuracy as defined by a hit-or-miss ratio
    val hitRight = knnRight.zipWithIndex.map { in => if (in._1.toInt == truth(in._2)) 1 else 0 }
    val hitLeft = knnLeft.zipWithIndex.map { in => if (in._1.toInt == truth(in._2)) 1 else 0 }
    val hitVec = hitLeft.zip(hitRight).map { in => if (in._1 == 1 || in._2 == 1) 1 else 0 }
    val hit = hitVec.sum / k.toDouble

    // Calculate the accuracy regardless of the position in the kNN vector.
    // The choice between left and right does not matter in this case.
    val count = knnRight.map { in => if (truth.contains(in.toInt)) 1 else 0 }.sum / k.toDouble

    // Latency metric
    val end = System.currentTimeMillis()
    val timeDiff = end - start

    // knnLeft is emitted as the result and is chosen over knnRight for no particular reason
    (queryPointID, timeDiff, hit, count, knnLeft.toVector)
  }
}
