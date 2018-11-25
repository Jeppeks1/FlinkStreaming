package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

/**
  * Takes a (queryPointID, time, Vector[(pointID, distance)]) and returns the results along with
  * a comparison to the ground truth. The output is a single record of (queryPointID, time, hit, recall)
  * where the time value is a latency measure, the hit and recall variables are accuracy metrics and
  * knn is the k-Nearest neighbors of the queryPoint.
  */
final class StreamingGroundTruth(groundTruth: Array[(Int, Array[Int])], k: Int)
  extends RichMapFunction[(Long, Long, Array[(Long, Double)]),
    (Long, Long, String, String)] {

  private val log: Logger = LoggerFactory.getLogger(StreamingGroundTruth.getClass)
  private var groundTruthSorted: Array[Array[Int]] = _

  override def open(parameters: Configuration): Unit = {
    // The ground truth file is read with a DataSet which is not deterministic
    // Sort the input based on the value attached to every vector, which is
    // the global index.
    groundTruthSorted = groundTruth.sortBy(_._1).map(_._2)
  }

  override def map(value: (Long, Long, Array[(Long, Double)])):
  (Long, Long, String, String) = {
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

    // Transform the hit-or-misses to a vector of metrics for each value of k
    val hitK = hitVec.zipWithIndex.map(in => hitVec.slice(0, in._2 + 1).sum.toDouble/(in._2 + 1))

    // Calculate the accuracy regardless of the position in the kNN vector.
    // The choice between left and right does not matter in this case.
    val recall = knnRight.zipWithIndex.map { in =>
      // Get the slices we are considering at the appropriate length
      val truthSlice = truth.slice(0, in._2 + 1)
      val resultSlice = knnRight.slice(0, in._2 + 1)

      // Compare the elements in the slices and calculate the recall metric
      resultSlice.map(res => if (truthSlice.contains(res.toInt)) 1 else 0).sum.toDouble / (in._2 + 1)
    }

    // Latency metric
    val time = System.currentTimeMillis()
    val timeDiff = time - value._2

    // Prepare the output for easier post-processing
    val hitK_out = formatOutput(hitK.toVector)
    val recall_out = formatOutput(recall.toVector)

    // Emit the result as (qpID, latency, accuracy, recall)
    (queryPointID, timeDiff, hitK_out, recall_out)
  }

  def formatOutput(vec: Vector[Double]): String = {
    val padding = Vector.fill[String](k - vec.size)("#I/T")
    val padded = vec.map("%.2f".format(_)) ++ padding
    padded.toString.replace(", ", ";").replace("Vector(", "").replace(")", "")
  }
}

// For the logger
object StreamingGroundTruth{

}