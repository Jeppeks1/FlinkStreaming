package project.distributed.functions

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import project.distributed.container.Point
import scala.collection.JavaConverters._


final class BatchSequentialScan extends RichFlatMapFunction[Point, (Long, Long, Double)] {

  private var queryPoints: Traversable[Point] = _

  override def open(parameters: Configuration): Unit = {
    queryPoints = getRuntimeContext.getBroadcastVariable[Point]("queryPoints").asScala
  }

  override def flatMap(input: Point, out: Collector[(Long, Long, Double)]): Unit = {
    // For the incoming point, calculate and emit the distance to all query points
    queryPoints.foreach { qp =>
      out.collect((qp.pointID, input.pointID, input.eucDist(qp)))
    }
  }
}