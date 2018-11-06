package project.distributed.reader

import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.commons.io.FilenameUtils
import project.distributed.container.Point


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
    * @param path The path to the .ivecs or .fvecs featureVector file.
    * @return A vector representation of all the points in the input file.
    */
  def readFeatureVector(path: String): Vector[Point] = {

    val points = FilenameUtils.getExtension(path) match {
      case "ivecs" => fi_vecs(path)
      case "fvecs" => fi_vecs(path)
      case x => throw new IOException("Error: Unsupported data input format '" + x + "' in readFeatureVector:" +
        "Supported file extensions are .fvecs and .ivecs.")
    }
    points
  }


  /**
    * Method for reading .ivecs and .fvecs files into a Vector of Points.
    * The integers are stored as floating point numbers for simplicity.
    * The dimension of the input vectors must be d = 128 and each vector
    * must have size 4 + 4 * d.
    *
    * @param path The path to the feature vector.
    * @return A Vector representation of all the points in the input file.
    */
  private def fi_vecs(path: String): Vector[Point] = {

    val ext = FilenameUtils.getExtension(path)

    val data_in = new DataInputStream(
      new BufferedInputStream(
        new FileInputStream(
          new File(path))))

    val tmpArray = ByteBuffer.allocate(516).array
    val buffer = ByteBuffer.wrap(tmpArray)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    var id = 0
    var tempVec = Vector[Point]()
    while (data_in.available > 0) {
      data_in.readFully(tmpArray)
      buffer.rewind()

      val dim = buffer.getInt
      if (dim != 128) throw new IOException("Error: Unexpected dimensionality d = " + dim + " of a feature vector.")

      var vec = Vector[Float]()
      while (vec.size < dim) {
        if (ext == "ivecs")
          vec = vec :+ buffer.getInt.toFloat
        else if (ext == "fvecs")
          vec = vec :+ buffer.getFloat
      }
      tempVec = tempVec :+ Point(id, vec)
      id = id + 1
    }

    data_in.close()
    tempVec
  }


  /**
    * Method for reading .ivecs groundtruth files into a Vector of Vectors.
    * The dimension of the input vectors must be d = 100 or d = 1000 and each
    * vector must have size 4 + 4 * d.
    *
    * @param path The path to the ground truth file.
    * @return A vector representation of all the points in the input file.
    */
  def ivecs_truth(path: String): Vector[Vector[Int]] = {

    val ext = FilenameUtils.getExtension(path)

    val data_in = new DataInputStream(
      new BufferedInputStream(
        new FileInputStream(
          new File(path))))

    // TODO: Fix hardcoding of 404 below
    val tmpArray = ByteBuffer.allocate(404).array
    val buffer = ByteBuffer.wrap(tmpArray)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    var tempVec = Vector[Vector[Int]]()
    while (data_in.available > 0) {
      data_in.readFully(tmpArray)
      buffer.rewind()

      val dim = buffer.getInt
      if (!(dim == 100 || dim == 1000))
        throw new IOException("Error: Unexpected dimensionality d = " + dim + " of a ground truth vector.")

      var vec = Vector[Int]()
      while (vec.size < dim) {
        vec = vec :+ buffer.getInt
      }
      tempVec = tempVec :+ vec
    }

    data_in.close()
    tempVec
  }

}







