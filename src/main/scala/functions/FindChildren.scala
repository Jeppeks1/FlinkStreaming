package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import container.InternalNode
import scala.collection.JavaConverters._

/**
  * Finds the nodes at the previous level, which had the current node as one of their
  * treeA closest nodes.
  */
final class FindChildren extends RichMapFunction[InternalNode, InternalNode] {

  private var parentNodes: Traversable[(InternalNode, Array[Long])] = _

  override def open(parameters: Configuration): Unit = {
    parentNodes = getRuntimeContext.getBroadcastVariable[(InternalNode, Array[Long])]("parentNodes").asScala
  }

  def map(in: InternalNode): InternalNode = {
    val parents = parentNodes.toArray
    InternalNode.findChildren(parents)(in.pointNode)
  }
}


