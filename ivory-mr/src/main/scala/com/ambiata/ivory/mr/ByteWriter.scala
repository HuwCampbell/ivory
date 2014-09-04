package com.ambiata.ivory.mr

object ByteWriter {

  def write(bytes: Array[Byte], b: Array[Byte], offset: Int): Unit =
    System.arraycopy(b, 0, bytes, offset, b.length)

  def writeStringUTF8(bytes: Array[Byte], s: String, offset: Int): Int = {
    val b = s.getBytes("UTF-8")
    write(bytes, b, offset)
    offset + b.length
  }

  def writeByte(bytes: Array[Byte], b: Byte, offset: Int): Unit =
    bytes(offset) = b

  def writeInt(bytes: Array[Byte], value: Int, offset: Int): Unit = {
    writeByte(bytes, (value >>> 24).toByte, offset)
    writeByte(bytes, ((value >>> 16) & 0xff).toByte, offset + 1)
    writeByte(bytes, ((value >>> 8) & 0xff).toByte, offset + 2)
    writeByte(bytes, (value & 0xff).toByte, offset + 3)
  }

  def writeShort(bytes: Array[Byte], value: Short, offset: Int): Unit = {
    writeByte(bytes, ((value >>> 8) & 0xff).toByte, offset)
    writeByte(bytes, (value & 0xff).toByte, offset + 1)
  }
}
