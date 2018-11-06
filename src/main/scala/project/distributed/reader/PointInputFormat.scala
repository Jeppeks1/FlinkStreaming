package project.distributed.reader

import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.commons.io.FilenameUtils
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.core.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}
import project.distributed.container.Point

/**
  * Class for reading the possibly very large input dataset in a distributed system in an efficient
  * and hopefully memory-safe way. The parallelism can be adjusted with the minNumSplits parameter
  * in the createInputSplits method. The default value is set to the number of threads on the current
  * machine, but can be manually increased with great effect, if memory management is an issue: More
  * splits allows the Garbage Collector to process some data and quickly remove it from the heap.
  *
  * The methods in a FileInputFormat are accessed in the following order:
  * <li>The FileInputFormat is instantiated and then configured with the Configuration.</li>
  * <li>The method createInputSplits is executed exactly once and then a number of worker
  * threads are spawned, each with a serialized instance that overwrites class variables.</li>
  * <li>Each worker instance configures their own instance according to the Configuration
  * and then opens the FileInputSplit.</li>
  * <li>The records are processed with the nextRecord method.</li>
  * <li>All records are read from the input when reachedEnd returns true.</li>
  * <li>The input format is closed.</li>
  * </ol>
  *
  */
class PointInputFormat(inputPath: Path) extends FileInputFormat[Point]{

  // Note: A single global path is used, as it is the same for every FileInputSplit instance

  protected val logger: Logger = LoggerFactory.getLogger(classOf[PointInputFormat])

  private var buffer: ByteBuffer = _
  private var targetID: Long = _
  private var pointID: Long = _

  override def configure(parameters: Configuration): Unit = {
    setFilePath(inputPath.makeQualified(inputPath.getFileSystem))
  }

  override def open(fileSplit: FileInputSplit): Unit = {
    // Re-calculate the number of points per split, as the value from createInputSplits is lost in serialization
    val ext = FilenameUtils.getExtension(inputPath.getPath)
    val recordSize = if (ext == "bvecs") 132 else 516
    val pointsPerSplit = fileSplit.getLength/recordSize

    // Get the points per split for this particular path
    val basePointsPerSplit = if (inputPath.toString.contains("query")) PointInputFormat.queryPPS else PointInputFormat.basePPS

    // Set the target and initial pointID
    targetID = fileSplit.getSplitNumber * basePointsPerSplit + pointsPerSplit
    pointID = fileSplit.getSplitNumber * basePointsPerSplit

    // Define a buffer which will contain the data read from this split
    val tmpArray = ByteBuffer.allocate(fileSplit.getLength.toInt).array
    val byteBuffer = ByteBuffer.wrap(tmpArray)
    buffer = byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

    // Hadoops implementation needed, as Flinks FSDataInputStream does not offer 'readFully'
    val hdfsPath = new org.apache.hadoop.fs.Path(inputPath.toString)
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()

    // Open the fileSplit in a FSDataInputStream (Hadoop version, not Flink)
    val fileSystem = FileSystem.get(inputPath.toUri, hdfsConfig)
    val hdfs = fileSystem.open(hdfsPath)

    // Read all the bytes, starting from the offset specified in the current split
    hdfs.readFully(fileSplit.getStart, tmpArray, 0, fileSplit.getLength.toInt)
  }

  // Class-wide parameters set in this method are overwritten by the default value, when the class is serialized
  override def createInputSplits(minNumSplits: Int): Array[FileInputSplit] = {
    if (minNumSplits < 1)
      throw new IllegalArgumentException("Number of input splits has to be at least 1.")

    // Prepare the path dependencies
    val fileSystem = inputPath.getFileSystem
    val fileStatus = fileSystem.getFileStatus(inputPath)
    val ext = FilenameUtils.getExtension(inputPath.getName)

    // Determine the number of points in each split based on minNumSplits
    val recordSize = if (ext == "bvecs") 132 else 516
    val pointsPerSplit = Math.floor(fileStatus.getLen.toDouble/minNumSplits/recordSize)
    val byteOverflow = fileStatus.getLen - pointsPerSplit * minNumSplits * recordSize

    // Set the base pointsPerSplit in the object of this class, for use in the open method
    if (inputPath.toString.contains("query"))
      PointInputFormat.queryPPS = pointsPerSplit.toInt
    else
      PointInputFormat.basePPS = pointsPerSplit.toInt


    // Define the size of each split and create a container for the splits
    val splitSize = (recordSize * pointsPerSplit).toLong
    var inputSplits = Array[FileInputSplit]()

    for (i <- 0 until minNumSplits){
      // Have the last split read the overflowing bytes, if there are any
      val overflow = if (i == minNumSplits - 1) byteOverflow.toLong else 0

      // Get the block locations and the Hosts containing this split
      val blocks = fileSystem.getFileBlockLocations(fileStatus, i * splitSize, splitSize + overflow)
      val hosts = blocks.flatMap(_.getHosts)

      val fis = new FileInputSplit(i, inputPath, i * splitSize, splitSize + overflow, hosts)
      inputSplits = inputSplits :+ fis
    }

    inputSplits
  }


  override def nextRecord(reuse: Point): Point = {
    var vec = Vector[Float]()

    // Read the first four bytes containing the dimension
    val dim = buffer.getInt
    if (dim != 128) throw new IOException("Error: Unexpected dimensionality d = " + dim + " of a feature vector.")

    // Read the remaining floats
    for (_ <- 0 until dim) vec = vec :+ buffer.getFloat

    // Define the point and increment the pointID
    val point = new Point(pointID, vec)
    pointID = pointID + 1
    point
  }

  override def reachedEnd(): Boolean = {
    if (pointID < targetID) false else true
  }

}

/**
  * This object is used to transfer a single static value to the open method of the class.
  * There are probably better ways to do this though.
  */
object PointInputFormat {
  var queryPPS: Int = _
  var basePPS: Int = _
}