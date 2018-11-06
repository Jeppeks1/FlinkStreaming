package project.distributed.functions

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector
import project.distributed.container.{InternalNode, Point}

/**
  * Flattens the incoming (Point, Vector[InternalNode]) records and retrieves the clusterID
  * from each InternalNode, to be used for grouping. The resulting type is (Point, clusterID).
  */
final class FlatMapper extends FlatMapFunction[(Point, Vector[InternalNode]), (Point, Long)] {

  def flatMap(input: (Point, Vector[InternalNode]),
              out: Collector[(Point, Long)]): Unit = {
    input._2.foreach { in =>
      out.collect((input._1, in.pointNode.pointID))
    }
  }
}