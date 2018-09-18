package project.distributed.reader

import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.flink.api.scala._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.commons.io.FilenameUtils
import project.distributed.container.Point
import org.apache.flink.api.common.functions._

/**
  * The basic abstraction for reading feature vectors into a DataSet
  * which allows for computations on huge amounts of data.
  */
object FeatureVector {

  /**
    * Method for reading feature vector files with the .ivecs and .fvecs extensions.
    * The integers are stored as floating point numbers for simplicity.
    * The dimension of the input vectors must be d = 128 and each vector
    * must have size 4 + 4 * d.
    *
    * @param env The current execution environment.
    * @param params ParameterTool containing --featureVector pathToFile
    * @return A vector representation of all the points in the input file.
    */
  def readFeatureVector(env: ExecutionEnvironment, params: ParameterTool): DataSet[Point] = {
    val path = params.get("featureVector")
    val points = FilenameUtils.getExtension(path) match {
      case "fvecs" => fi_vecs(path, env)
      case "ivecs" => fi_vecs(path, env)
      case _ => throw new IOException("Error: Unsupported data input format in readFeatureVector:\n" +
        "Supported file extensions are .fvecs and .ivecs.")
    }
    points
  }


  /**
    * Method for reading feature vector files with the .ivecs and .fvecs extensions.
    * The integers are stored as floating point numbers for simplicity.
    * The dimension of the input vectors must be d = 128 and each vector
    * must have size 4 + 4 * d.
    *
    * @param env The current execution environment.
    * @param params ParameterTool containing --queryPoint pathToFile
    * @return A vector representation of all the points in the input file.
    */
  def readQueryPoints(env: ExecutionEnvironment, params: ParameterTool): DataSet[Point] = {
    val path = params.get("queryPoint")
    val points = FilenameUtils.getExtension(path) match {
      case "fvecs" => fi_vecs(path, env)
      case "ivecs" => fi_vecs(path, env)
      case _ => throw new IOException("Error: Unsupported data input format in readFeatureVector:\n" +
        "Supported file extensions are .fvecs and .ivecs.")
    }
    points
  }

  /**
    * Method for reading .ivecs and .fvecs files into a vector of Points.
    * The integers are stored as floating point numbers for simplicity.
    * The dimension of the input vectors must be d = 128 and each vector
    * must have size 4 + 4 * d.
    * @param filename The path to the feature vector.
    * @param env The current execution environment.
    * @return A vector representation of all the points in the input file.
    */
  private def fi_vecs(filename: String, env: ExecutionEnvironment): DataSet[Point] = {

    val ext = FilenameUtils.getExtension(filename)

    val data_in = new DataInputStream(
      new BufferedInputStream(
        new FileInputStream(
          new File(filename))))

    // It appears a DataSet must be non-empty on creation, so we use a placeholder
    var fvecs: DataSet[Point] = env.fromElements(Point(-1, Vector()))
    val tmpArray = ByteBuffer.allocate(516).array
    val buffer = ByteBuffer.wrap(tmpArray)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    var id = 0
    while (data_in.available > 0) {
      data_in.readFully(tmpArray)
      buffer.rewind()

      val dim = buffer.getInt
      if (dim != 128) throw new IOException("Error: Unexpected dimensionality of a feature vector.")

      var vec = Vector[Float]()
      while (vec.size < dim) {
        if (ext == "ivecs")
          vec = vec :+ buffer.getInt.toFloat
        else if (ext == "fvecs")
          vec = vec :+ buffer.getFloat
      }
      // TODO: Fix stack safety issue
      fvecs = fvecs.union(env.fromElements(Point(id, vec)))
      id = id + 1
    }

    fvecs.filter(_.pointID != -1)
  }



}




