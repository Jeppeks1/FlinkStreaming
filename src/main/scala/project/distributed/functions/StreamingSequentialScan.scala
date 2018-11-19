package project.distributed.functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.slf4j.{Logger, LoggerFactory}
import project.distributed.container.Point

/**
  * Performs a sequential scan in a streaming environment to determine the k-Nearest neighbors
  * of the incoming query point. The result is a single record containing
  * (queryPointID, time, Vector[(pointID, distance)]) where time is a latency measure and the
  * vector contains the k-Nearest neighbors and their distances.
  *
  * @param k The parameter determining the number of nearest neighbors to return.
  */
final class StreamingSequentialScan(pointsInput: Array[Point], k: Int) extends RichMapFunction[Point, (Long, Long, Array[(Long, Double)])] {

  private val log: Logger = LoggerFactory.getLogger(StreamingSequentialScan.getClass)

  override def map(input: Point): (Long, Long, Array[(Long, Double)]) = {
    // Latency metric
    val time = System.currentTimeMillis()

    // For the incoming query point, calculate and return the distance to the nearest points
    val points = pointsInput.map{ p => (p.pointID, input.eucDist(p)) }
    (input.pointID, time, points.sortBy(_._2).slice(0, k))
  }
}

object StreamingSequentialScan{
  var pointsStatic: Array[Point] = _
}
