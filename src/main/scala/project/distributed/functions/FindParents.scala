package project.distributed.functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import project.distributed.container.InternalNode
import scala.collection.JavaConverters._

/**
  * Finds the treeA closest parent nodes for the incoming point.
  */
final class FindParents(treeA: Int) extends RichMapFunction[(Int, InternalNode), (InternalNode, Array[Long])] {

  private var newNodes: Traversable[(Int, InternalNode)] = _

  // The broadcasted set contains the nodes selected for the next level
  override def open(parameters: Configuration): Unit = {
    newNodes = getRuntimeContext.getBroadcastVariable[(Int, InternalNode)]("newNodes").asScala
  }

  // The Vector[Long] has size at most treeA and contains the pointIDs of the nearest nodes.
  def map(in: (Int, InternalNode)): (InternalNode, Array[Long]) = {
    val current = newNodes.map(_._2).toArray
    (in._2, InternalNode.findParents(current, treeA)(in._2.pointNode))
  }
}
