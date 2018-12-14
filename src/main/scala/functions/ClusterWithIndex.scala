package functions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

import container.InternalNode.clusterWithIndex
import container.InternalNode
import container.Point

/**
  * Cluster the incoming points with the broadcasted root or randomly to clusterSize clusters.
  *
  * @param clusterSize The number of clusters to use when clustering in the full scan method.
  * @param a The number of clusters associated with each point.
  */
final class ClusterWithIndex(clusterSize: Integer, a: Int) extends RichFlatMapFunction[Point, (Point, Long)] {

  private val log: Logger = LoggerFactory.getLogger(classOf[ClusterWithIndex])
  private var root: Option[InternalNode] = _
  private var processedPoints: Int = 0

  override def open(parameters: Configuration): Unit = {
    root = getRuntimeContext.getBroadcastVariable[InternalNode]("root").asScala.headOption
  }

  override def flatMap(point: Point, out: Collector[(Point, Long)]): Unit = {
    if (root.isEmpty){
      // Perform the clustering randomly to clusterSize clusters - used in the full scan
      out.collect((point, math.ceil(math.random * clusterSize).toLong))
    } else {
      // Use the root node to guide the index - used in the index search
      val clustered = clusterWithIndex(root.get, point, a)
      clustered.foreach{ in =>
        out.collect((point, in.pointNode.pointID))
      }
    }

    // The clustering phase can be really slow, so the progress is written to the log.
    processedPoints = processedPoints + 1
    if (processedPoints % 100000 == 0)
      log.info("ClusterWithIndex " + getRuntimeContext.getIndexOfThisSubtask + "/" +
        getRuntimeContext.getNumberOfParallelSubtasks + " has processed " + processedPoints + " points.")

  }
}
