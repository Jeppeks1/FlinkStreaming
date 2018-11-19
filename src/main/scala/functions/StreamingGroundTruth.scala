package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

/**
  * Takes a (queryPointID, time, Vector[(pointID, distance)]) and returns the results along with
  * a comparison to the ground truth. The output is a single record of (queryPointID, time, hit, recall, knn)
  * where the time value is a latency measure, the hit and recall variables are accuracy metrics and
  * knn is the k-Nearest neighbors of the queryPoint.
  */
final class StreamingGroundTruth(groundTruth: Array[(Int, Array[Int])], k: Int)
    extends RichMapFunction[(Long, Long, Array[(Long, Double)]), (Long, Long, Double, Double, Vector[Long])] {

  private val log: Logger = LoggerFactory.getLogger(StreamingGroundTruth.getClass)
  private var groundTruthSorted: Array[Array[Int]] = _

  override def open(parameters: Configuration): Unit = {
    // The ground truth file is read with a DataSet which is not deterministic
    // Sort the input based on the value attached to every vector, which is
    // the global index.
    groundTruthSorted = groundTruth.sortBy(_._1).map(_._2)
  }

  override def map(value: (Long, Long, Array[(Long, Double)])): (Long, Long, Double, Double, Vector[Long]) = {
    // Get the query point from the input
    val queryPointID = value._1

    // Get the first k elements from the ground truth vector
    val truth = groundTruthSorted(queryPointID.toInt).slice(0, k)

    // There is no way to know in which order two points occur in the groundTruth,
    // if they have the same distance to the queryPoint, so we sort first by distance,
    // and then on the pointID in both directions.
    val knnRight = value._3.sortBy(in => (in._2, -in._1)).map(_._1)
    val knnLeft = value._3.sortBy(in => (in._2, in._1)).map(_._1)

    // Calculate the accuracy as defined by a hit-or-miss ratio
    val hitRight = knnRight.zipWithIndex.map { in => if (in._1.toInt == truth(in._2)) 1 else 0 }
    val hitLeft = knnLeft.zipWithIndex.map { in => if (in._1.toInt == truth(in._2)) 1 else 0 }
    val hitVec = hitLeft.zip(hitRight).map { in => if (in._1 == 1 || in._2 == 1) 1 else 0 }
    val hit = hitVec.sum / k.toDouble

    // Calculate the accuracy regardless of the position in the kNN vector.
    // The choice between left and right does not matter in this case.
    val count = knnRight.map { in => if (truth.contains(in.toInt)) 1 else 0 }.sum / k.toDouble

    // Latency metric
    val time = System.currentTimeMillis()
    val timeDiff = time - value._2

    // knnLeft is emitted as the result and is chosen over knnRight for no particular reason
    (queryPointID, timeDiff, hit, count, knnLeft.toVector)
  }
}

// For the logger
object StreamingGroundTruth{

}