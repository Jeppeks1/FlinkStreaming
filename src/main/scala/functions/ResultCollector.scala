package functions

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
import java.lang.Iterable


/**
  * Collects an input (queryPointID, pointID, distance) grouped on queryPointID and produces
  * a single output. The result is a (queryPointID, Vector[(pointID, distance)]) record,
  * which is a vector of distances from the queryPointID to the pointID.
  */
final class ResultCollector extends GroupReduceFunction[(Long, Long, Double), (Long, Array[(Long, Double)])] {

  override def reduce(input: Iterable[(Long, Long, Double)],
                      out: Collector[(Long, Array[(Long, Double)])]): Unit = {
    val data = input.iterator().asScala.toArray
    val queryPointID = data.head._1
    val knn = data.map(in => (in._2, in._3))
    out.collect((queryPointID, knn))
  }
}
