package project.distributed.functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import project.distributed.container.InternalNode
import scala.collection.JavaConverters._

/**
  * Finds the nodes at the previous level, which had the current node as one of their
  * treeA closest nodes.
  */
final class FindChildren extends RichMapFunction[(Int, InternalNode), (Int, InternalNode)] {

  private var parentNodes: Traversable[(InternalNode, Array[Long])] = _

  override def open(parameters: Configuration): Unit = {
    parentNodes = getRuntimeContext.getBroadcastVariable[(InternalNode, Array[Long])]("parentNodes").asScala
  }

  def map(in: (Int, InternalNode)): (Int, InternalNode) = {
    val parents = parentNodes.toArray
    val level = in._1
    (level + 1, InternalNode.findChildren(parents)(in._2.pointNode))
  }
}


