package project.distributed.reader

import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.commons.io.FilenameUtils
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.core.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.flink.configuration.Configuration

/**
  * Class for reading the ground truth files found on HDFS. See PointInputFormat for
  * a full explanation of the FileInputFormat.
  */
class TruthInputFormat(inputPath: Path) extends FileInputFormat[(Int, Array[Int])] {

  // Note: A single global path is used, as it is the same for every FileInputSplit instance

  private val recordSize: Int = 100 * 4 + 4 // 100 integers with another integer containing the dim
  private var buffer: ByteBuffer = _
  private var bytesLeft: Long = _
  private var index: Int = _

  override def configure(parameters: Configuration): Unit = {
    setFilePath(inputPath.makeQualified(inputPath.getFileSystem))
  }

  override def open(fileSplit: FileInputSplit): Unit = {
    // Set the amount of bytes remaining
    bytesLeft = fileSplit.getLength

    // Define a buffer which will contain the data read from this split
    val tmpArray = ByteBuffer.allocate(fileSplit.getLength.toInt).array
    val byteBuffer = ByteBuffer.wrap(tmpArray)
    buffer = byteBuffer.order(ByteOrder.LITTLE_ENDIAN)

    // Prepare a counter that determines the global index of the ground truth vector
    index = fileSplit.getSplitNumber * (100 / 4) // Hardcoding: (number of Vectors = 100 / minNumSplits = 4)

    // Hadoops implementation needed, as Flinks FSDataInputStream does not offer 'readFully'
    val hdfsPath = new org.apache.hadoop.fs.Path(inputPath.toString)
    val hdfsConfig = new org.apache.hadoop.conf.Configuration()

    // Open the fileSplit in a FSDataInputStream (Hadoop version, not Flink)
    val fileSystem = FileSystem.get(inputPath.toUri, hdfsConfig)
    val hdfs = fileSystem.open(hdfsPath)

    // Read all the bytes, starting from the offset specified in the current split
    hdfs.readFully(fileSplit.getStart, tmpArray, 0, fileSplit.getLength.toInt)
  }

  override def createInputSplits(minNumSplits: Int): Array[FileInputSplit] = {
    // Prepare the path dependencies
    val fileSystem = inputPath.getFileSystem
    val fileStatus = fileSystem.getFileStatus(inputPath)
    val ext = FilenameUtils.getExtension(inputPath.getName)
    if (ext != "ivecs") throw new IOException("Error: Unknown extension " + ext + " passed to method expecting an .ivecs file")

    // Define the size of each split and create a container for the splits
    val splitSize = fileStatus.getLen/minNumSplits
    var inputSplits = Array[FileInputSplit]()

    // Make sure the splitSize is an integer
    if (splitSize % 1 != 0)
      throw new Exception("Error: Unbalanced splitSize - handle the overflow as in PointInputFormat")

    // Define the splits
    for (i <- 0 until minNumSplits){
      // Get the block locations and the Hosts containing this split
      val blocks = fileSystem.getFileBlockLocations(fileStatus, i * splitSize, splitSize)
      val hosts = blocks.flatMap(_.getHosts)

      val fis = new FileInputSplit(i, inputPath, i * splitSize, splitSize, hosts)
      inputSplits = inputSplits :+ fis
    }

    inputSplits
  }


  override def nextRecord(reuse: (Int, Array[Int])): (Int, Array[Int]) = {
    var vec = Array[Int]()

    // Read the first four bytes containing the dimension
    val dim = buffer.getInt
    if (!(dim == 100 || dim == 1000))
      throw new IOException("Error: Unexpected dimensionality d = " + dim + " of a ground truth vector.")

    // Read the remaining integers
    for (_ <- 0 until dim) vec = vec :+ buffer.getInt

    // Update the value used in the stopping condition
    bytesLeft = bytesLeft - recordSize

    // Prepare the result and increment the index counter
    val result = (index, vec)
    index = index + 1
    result
  }

  override def reachedEnd(): Boolean = {
    if (bytesLeft > 0) false else true
  }

}
