package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import container.InternalNode
import scala.collection.JavaConverters._

/**
  * Finds the treeA closest parent nodes for the incoming point.
  */
final class FindParents(treeA: Int) extends RichMapFunction[InternalNode, (InternalNode, Array[Long])] {

  private var newNodes: Traversable[InternalNode] = _

  // The broadcasted set contains the nodes selected for the next level
  override def open(parameters: Configuration): Unit = {
    newNodes = getRuntimeContext.getBroadcastVariable[InternalNode]("newNodes").asScala
  }

  // The Array[Long] has size at most treeA and contains the pointIDs of the nearest nodes.
  def map(in: InternalNode): (InternalNode, Array[Long]) = {
    val current = newNodes.toArray
    (in, InternalNode.findParents(current, treeA)(in.pointNode))
  }
}
