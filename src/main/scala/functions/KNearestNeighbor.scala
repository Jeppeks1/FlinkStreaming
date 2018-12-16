package functions

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path

import org.slf4j.{Logger, LoggerFactory}

import container.InternalNode.searchTheIndex
import container.{InternalNode, Point}
import reader.ClusterInputFormat

/**
  * Determines the K-Nearest neighbors for each incoming point. The input is
  * (queryPoint, Time, Vector[InternalNode]) which contains the clusterIDs to be
  * searched through. The result is (queryPointID, Vector[(pointID, distance)])
  * which is a vector of distances from the queryPointID to the pointID.
  *
  * This class is used for the file-based streaming approach.
  *
  * @param root        The root node of the index.
  * @param leafs       Array of Points containing the cluster leaders at the buttom level of the index.
  * @param clusterPath The base path to the directory where the clustered points are written.
  * @param b           The number of nearest clusters to search through for each query point.
  * @param k           The parameter determining the number of nearest neighbors to return.
  */
final class KNearestNeighbor(root: InternalNode,
                             leafs: Array[Point],
                             clusterPath: Path,
                             b: Int,
                             k: Int) extends RichMapFunction[Point, (Long, Long, Array[(Long, Double)])] {

  private val log: Logger = LoggerFactory.getLogger(classOf[KNearestNeighbor])
  private var clusteredFiles: Array[Long] = _

  override def open(parameters: Configuration): Unit = {
    // Prepare the HDFS path dependencies
    val hdfsPath = new org.apache.hadoop.fs.Path(clusterPath.toString)
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()
    val fileSystem = hdfsPath.getFileSystem(hdfsConfig)
    val listStatus = fileSystem.listStatus(hdfsPath).filter(!_.getPath.toString.contains("indexLeafs"))

    // Some incoming query points are guided to a cluster that was not written,
    // as the cluster leader of the cluster took a wrong turn somewhere in the
    // clustering process. Find all clusterIDs that was actually written.
    clusteredFiles = listStatus.map { in =>
      val path = in.getPath.toString
      val split = path.split("-")

      if (split.length != 2)
        throw new Exception("Error: Unexpected split " + split.toVector)

      split(2).toLong
    }

    // Find a list of all the clusterIDs that was not written to a disk.
    val diff = leafs.filter(id => !clusteredFiles.contains(id.pointID)).map(_.pointID)

    // Log the difference with a warning message.
    if (diff.length > 0)
      log.warn("Warning: " + diff.length + " cluster leaders was not written to the disk.")
  }

  override def map(qp: Point): (Long, Long, Array[(Long, Double)]) = {
    // Latency metric
    val time = System.currentTimeMillis()

    // Determine the b closest clusters to the query point based on the distance to the cluster leader
    var clusterIDs = if (root != null) {
      // Use the index to determine the nearest clusterIDs
      searchTheIndex(root, null)(qp, b)
    } else {
      // Scan the leafs to determine the nearest clusterIDs
      leafs
        .map { p => (p.pointID, p.eucDist(qp)) }
        .sortBy(_._2)
        .map(_._1)
        .distinct
        .slice(0, b)
    }

    // Remove the clusterIDs that was not actually written to disk
    clusterIDs = clusterIDs.filter(id => clusteredFiles.contains(id))

    // Workaround 2 reads the clustered points from a file named after the clusterID
    // Create a placeholder for the kNN, which will be updated after each iteration.
    var currentKNN = Array[(Long, Double)]()

    clusterIDs.foreach { id =>
      // Define the path to read
      val path = new Path(clusterPath, "clusterID-" + id)

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