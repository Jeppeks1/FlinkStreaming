package functions

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import container.Point
import scala.math.{ceil, floor, pow}

/**
  * Determines a set of randomly chosen Points, that makes up the buttom layer of
  * the index. The pointID is used as the clusterID when converted to an InternalNode
  * by the next operator. The size of the output varies, but is roughly equal to the value
  * contained in either of the leaderCount variables, which is determined based on the
  * CP or eCP methods.
  *
  * @param inputSize The number of elements in the points set.
  * @param recordSize The size of a single record when written to disk.
  * @param L The number of desired levels in the index.
  */
final class SelectRandomLeafs(inputSize: Long, recordSize: Int, L: Int) extends RichFilterFunction[Point] {

  private var reductionFactor: Double = _

  override def open(parameters: Configuration): Unit = {
    // Set some constants used in the eCP approach
    val balancingFactor = 0        // Pick an additional balancingFactor % leafs
    val IOGranularity = 128 * 1024 // 128 KB IO granularity on a Linux OS

    // Determine the leaderCount based on the CP approach (for debugging) or
    // the eCP approach (for the serious experiments).
    val leaderCount_eCP = ceil(inputSize / floor(IOGranularity / recordSize)).toInt
    val leaderCount_CP = ceil(pow(inputSize, 1 - 1.0 / (L + 1))).toInt

    // Adjust the count based on the balancingFactor
    val leafCount = ceil(leaderCount_eCP + leaderCount_eCP * balancingFactor).toInt

    // Set the reduction factor
    reductionFactor = leafCount / inputSize.toDouble
  }

  def filter(in: Point): Boolean = {
    // Generate a random number, that determines if the input should be kept or not
    if (math.random < reductionFactor) true else false
  }

}
