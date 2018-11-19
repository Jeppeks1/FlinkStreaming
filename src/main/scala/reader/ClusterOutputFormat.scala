package reader

import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.core.fs.Path
import container.Point
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.common.io.SerializedOutputFormat
import org.slf4j.{Logger, LoggerFactory}


class ClusterOutputFormat(basePath: Path) extends FileOutputFormat[(Point, Long)]{

  protected val log: Logger = LoggerFactory.getLogger(classOf[ClusterOutputFormat])

  var format: SerializedOutputFormat[Point] = _
  var currentClusterID: Long = -1

  override def initializeGlobal(parallelism: Int): Unit = {
    // The outer output format should not be globally initialized.
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    // The outer output format should not be opened.
  }

  override def writeRecord(record: (Point, Long)): Unit = {

    if (record._2 != currentClusterID || format == null){
      // The writing to the previous format has finished or a format has not been set yet
      currentClusterID = record._2
      if (format != null) format.close()

      // Set a new output format at for example basePath/clusterID-5
      val path = new Path(basePath, "clusterID-" + record._2)
      format = new SerializedOutputFormat[Point]

      // Initialize the format
      format.setOutputFilePath(path)
      format.setWriteMode(WriteMode.OVERWRITE)
      format.setOutputDirectoryMode(FileOutputFormat.OutputDirectoryMode.PARONLY)
      format.open(getRuntimeContext.getIndexOfThisSubtask, getRuntimeContext.getNumberOfParallelSubtasks)
    }

    // Write the point to the currently opened file
    val point = record._1
    format.writeRecord(point)
  }

  override def close(): Unit = {
    super.close()

    if (format != null)
      format.close()
  }

}
