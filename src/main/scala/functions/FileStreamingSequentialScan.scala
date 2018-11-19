package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.hadoop.fs.FileStatus

import org.slf4j.{Logger, LoggerFactory}

import reader.ClusterInputFormat
import container.Point

/**
  * Performs a sequential scan in a streaming environment to determine the k-Nearest neighbors
  * of the incoming query point. The result is a single record containing
  * (queryPointID, time, Vector[(pointID, distance)]) where time is a latency measure and the
  * vector contains the k-Nearest neighbors and their distances.
  *
  * @param clusterPath The path to the directory containing the clusters.
  * @param k The parameter determining the number of nearest neighbors to return.
  */
final class FileStreamingSequentialScan(clusterPath: Path, k: Int) extends RichMapFunction[Point, (Long, Long, Array[(Long, Double)])] {

  private val log: Logger = LoggerFactory.getLogger(classOf[FileStreamingSequentialScan])
  private var listStatus: Array[FileStatus] = _

  override def open(parameters: Configuration): Unit = {
    // Prepare the HDFS path dependencies
    val hdfsPath = new org.apache.hadoop.fs.Path(clusterPath.toString)
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()
    val fileSystem = hdfsPath.getFileSystem(hdfsConfig)
    listStatus = fileSystem.listStatus(hdfsPath)
  }

  override def map(qp: Point): (Long, Long, Array[(Long, Double)]) = {
    // Latency metric
    val time = System.currentTimeMillis()

    // Workaround 2 reads the clustered points from a file named after the clusterID
    // Create a placeholder for the kNN, which will be updated after each iteration.
    var currentKNN = Array[(Long, Double)]()

    listStatus.foreach { inputPath =>
      // Get the path to be read
      val path = new Path(inputPath.getPath.toUri)

      // Read the clustered points and calculate the kNN within´the cluster
      val cpKNN = ClusterInputFormat.readCluster(path)
        .map(cp => (cp.pointID, qp.eucDist(cp)))
        .distinct
        .sortBy(_._2)
        .slice(0, k)

      // Combine the two arrays of kNNs into one
      if (currentKNN == null) {
        currentKNN = cpKNN
      } else {
        currentKNN = (currentKNN ++ cpKNN)
          .distinct
          .sortBy(_._2)
          .slice(0, k)
      }
    }

    (qp.pointID, time, currentKNN)
  }
}


