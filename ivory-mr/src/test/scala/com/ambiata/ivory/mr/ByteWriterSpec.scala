package com.ambiata.ivory.mr

import org.apache.hadoop.io.WritableComparator
import org.scalacheck._
import org.specs2._

class ByteWriterSpec extends Specification with ScalaCheck { def is = s2"""

  Symmetric bytes                               $symmetricBytes
  Symmetric byte                                $symmetricByte
  Symmetric string                              $symmetricString
  Symmetric short                               $symmetricShort
  Symmetric int                                 $symmetricInt

"""

  def symmetricBytes = prop((i: Array[Byte], offset: PositiveShort) => {
    val bytes = new Array[Byte](offset.s + i.length)
    ByteWriter.write(bytes, i, offset.s)
    new String(bytes, offset.s, i.length) ==== new String(i)
  })

  def symmetricByte = prop((i: Byte, offset: PositiveShort) => {
    val bytes = new Array[Byte](offset.s + 1)
    ByteWriter.writeByte(bytes, i, offset.s)
    bytes(offset.s) ==== i
  })

  def symmetricString = prop((i: String, offset: PositiveShort) => {
    val length = i.getBytes("UTF-8").length
    val bytes = new Array[Byte](offset.s + length)
    val newOffset = ByteWriter.writeStringUTF8(bytes, i, offset.s)
    new String(bytes, offset.s, newOffset - offset.s, "UTF-8") ==== i
  })

  def symmetricShort = prop((i: Short, offset: PositiveShort) => {
    val bytes = new Array[Byte](offset.s + 2)
    ByteWriter.writeShort(bytes, i, offset.s)
    WritableComparator.readUnsignedShort(bytes, offset.s).toShort === i
  })

  def symmetricInt = prop((i: Int, offset: PositiveShort) => {
    val bytes = new Array[Byte](offset.s + 4)
    ByteWriter.writeInt(bytes, i, offset.s)
    WritableComparator.readInt(bytes, offset.s) === i
  })

  case class PositiveShort(s: Short)
  implicit def PositiveShortArb: Arbitrary[PositiveShort] =
    Arbitrary(Gen.chooseNum[Short](0, Short.MaxValue).map(PositiveShort.apply))
}
