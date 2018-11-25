package functions

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.configuration.Configuration
import container.InternalNode
import scala.collection.JavaConverters._

import scala.math.{ceil, pow}

/**
  * Selects a set of randomly chosen nodes, to make up the nodes at the next level of
  * the index. The number of chosen nodes varies, but is roughly equal to the number
  * of nodes used at the current level in the CP method.
  *
  * @param inputSize The number of elements in the points set.
  * @param L The number of desired levels in the index.
  */
final class SelectRandomNodes(inputSize: Long, L: Int) extends RichFilterFunction[InternalNode] {

  private var reductionFactor: Double = _

  override def open(parameters: Configuration): Unit = {
    val currentNodes = getRuntimeContext.getBroadcastVariable[InternalNode]("currentNodes").asScala

    val level = currentNodes.head.level + 1
    val size = currentNodes.size

    // Use the CP approach to determine the number of nodes at the next level
    var nodeCount = ceil(pow(inputSize, 1 - level.toDouble / (L.toDouble + 1))).toInt

    // The disrepancy between methods in eCP and DeCP gives rises to cases, where the leader count
    // at the current level is higher than the count on the level before. Adjust for this case by
    // setting a default reduction factor of 40 percent.
    if (nodeCount > size) nodeCount = ceil(size * 0.6).toInt

    reductionFactor = nodeCount.toDouble / size.toDouble
  }

  def filter(in: InternalNode): Boolean = {
    // Generate a random number, that determines if the input should be kept or not
    if (math.random < reductionFactor) true else false
  }

}
