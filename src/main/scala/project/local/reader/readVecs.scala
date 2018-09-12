package project.local.reader

import project.local.container.SiftDescriptor
import java.io._
import java.nio._


object readVecs extends App {


  def ivecs(filename: String): Vector[SiftDescriptor[Int]] = {
    val data_in = new DataInputStream(
      new BufferedInputStream(
        new FileInputStream(
          new File(filename))))

    var ivecs = Vector[SiftDescriptor[Int]]()

    while (data_in.available > 0) {
      val dim = reverseEnddian(data_in.readInt()) // First byte gives the dimension
      var vec = Vector[Int]()
      while (vec.size < dim) { // Read the remaining d * 4 bytes
        vec = vec :+ reverseEnddian(data_in.readInt())
      }
      ivecs = ivecs :+ SiftDescriptor(-1, vec)
    }
    ivecs
  }

  def fvecs(filename: String): Vector[SiftDescriptor[Float]] = {

    val data_in = new DataInputStream(
      new BufferedInputStream(
        new FileInputStream(
          new File(filename))))

    var fvecs = Vector[SiftDescriptor[Float]]()
    val tmpArray = ByteBuffer.allocate(516).array
    val buffer = ByteBuffer.wrap(tmpArray)
    buffer.order(ByteOrder.LITTLE_ENDIAN)

    while (data_in.available > 0) {
      data_in.readFully(tmpArray)
      buffer.rewind()

      val dim = buffer.getInt
      if (dim != 128) throw new IOException("Unexpected length.")

      var vec = Vector[Float]()
      while (vec.size < dim) {
        vec = vec :+ buffer.getFloat
      }
      fvecs = fvecs :+ SiftDescriptor(-1, vec)
    }
    fvecs
  }


  def reverseEnddian(in: Int): Int = {
    (0x000000ff & (in >> 24)) |
      (0x0000ff00 & (in >> 8)) |
      (0x00ff0000 & (in << 8)) |
      (0xff000000 & (in << 24))
  }


  override def main(args: Array[String]): Unit = {
    //val ivec = ivecs("./data/siftsmall/siftsmall_query.fvecs")
    val fvec = fvecs("./data/siftsmall/siftsmall_query.fvecs")
    println(fvec(0))
    println(fvec(1))
    println(fvec(2))
    println(fvec.size)
  }
}
