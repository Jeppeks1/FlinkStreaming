package project.distributed.reader

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.DataSet
import org.apache.commons.io.FilenameUtils
import project.distributed.container._
import org.apache.flink.api.scala._

import java.nio.{ByteBuffer, ByteOrder}
import java.io._

object FeatureVector {

  def readFeatureVector(params: ParameterTool, env: ExecutionEnvironment): DataSet[Point] = {
    val points = FilenameUtils.getExtension(params.get("featureVector")) match {
      case "fvecs" => fi_vecs(params, env, BasicTypeInfo.FLOAT_TYPE_INFO)
      case "ivecs" => fi_vecs(params, env, BasicTypeInfo.INT_TYPE_INFO)
      case _ => throw new IOException("Error: Unsupported data input format in readFeatureVector:\n" +
        "Supported file extensions are .fvecs and .ivecs.")
    }
    points
  }

  private def fi_vecs(params: ParameterTool, env: ExecutionEnvironment, typeInfo: BasicTypeInfo[_]): DataSet[Point] = {

    val filename = params.get("featureVector")
    val ext = FilenameUtils.getExtension(params.get("featureVector"))

    val data_in = new DataInputStream(
      new BufferedInputStream(
        new FileInputStream(
          new File(filename))))

    var fvecs = Vector[Point]()
    val tmpArray = ByteBuffer.allocate(516).array // fvecs and ivecs have size 4 + 4 * d
    val buffer = ByteBuffer.wrap(tmpArray)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    var id = 0
    while (data_in.available > 0) {
      data_in.readFully(tmpArray)
      buffer.rewind()

      val dim = buffer.getInt
      if (dim != 128) throw new IOException("Error: Unexpected dimensionality of a feature vector.")

      val vec = FloatingPoint(id, Vector())
      while (vec.descriptor.size < dim) {
        if (ext == "ivecs")
          vec.descriptor :+ buffer.getInt
        else if (ext == "fvecs")
          vec.descriptor :+ buffer.getFloat
      }
      fvecs = fvecs :+ vec
      id = id + 1
    }
    env.fromCollection(fvecs)
  }
}



