package reader

import org.apache.flink.core.fs.Path
import container.Point
import org.apache.hadoop.fs.FileSystem
import java.nio.ByteBuffer
import org.slf4j.{Logger, LoggerFactory}


/**
  * This "InputFormat" must be used within a DataStream of query points.
  * It is therefore not possible to use Flinks normal InputFormats, so
  * the following object defines a vanilla method to read the data that
  * was written by ClusterOutputFormat
  */
object ClusterInputFormat {

  protected val log: Logger = LoggerFactory.getLogger(classOf[ClusterOutputFormat])

  // May throw a FileNotFoundException if the path was not found. This can happen if
  // the index guided none of the clustered points to a specific leaf, but then a query
  // point arrives and is guided to that cluster, which has not been written. The cause
  // is likely a value L (level) that is too high, leading to a thinly spread clustering.
  // Update: The above issue has been handled explicitly and an error will be thrown.
  def readCluster(inputPath: Path): Array[Point] = {

    // Hadoops implementation needed, as Flinks FSDataInputStream does not offer 'readFully'
    val hdfsPath = new org.apache.hadoop.fs.Path(inputPath.toString)
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()

    // Get the fileSystem and fileStatus and then open the file
    val fileSystem = FileSystem.get(inputPath.toUri, hdfsConfig)
    val fileStatus = fileSystem.getFileStatus(hdfsPath)
    val hdfs = fileSystem.open(hdfsPath)

    assert(fileStatus.getLen < Integer.MAX_VALUE)

    // Define the byte array which will contain the data
    val tmpArray = ByteBuffer.allocate(fileStatus.getLen.toInt).array
    val buffer = ByteBuffer.wrap(tmpArray)

    // Read all the bytes, starting from the beginning of the file and to the end
    hdfs.readFully(0, tmpArray, 0, fileStatus.getLen.toInt)
    hdfs.close()

    // Get the points belonging to this cluster
    var tempVec = Array[Point]()
    var dimCount = 0
    var bytesLeft = fileStatus.getLen - 24 // There is an overhead of 24 bytes somewhere
    while (bytesLeft > 0) {
      val pointID = buffer.getLong

      var descriptor = Array[Float]()
      while (dimCount < 128){
        val float = buffer.getChar.toFloat
        descriptor = descriptor :+ float
        dimCount = dimCount + 1
      }

      tempVec = tempVec :+ new Point(pointID, descriptor)
      bytesLeft = bytesLeft - (128 * 2 + 8) // 128 chars of two bytes each, eight bytes from the Long
      dimCount = 0
    }
    tempVec
  }


}


