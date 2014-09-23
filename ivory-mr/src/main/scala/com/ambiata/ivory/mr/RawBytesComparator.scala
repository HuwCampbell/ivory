package com.ambiata.ivory.mr

import org.apache.hadoop.io.{BytesWritable, RawComparator, WritableComparator}

/**
 * Helper util class for implementing [[RawComparator]].
 * This is mostly to strip off the extra 4 bytes from the base `compare()` method.
 */
abstract class RawBytesComparator extends RawComparator[BytesWritable] {

  def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int

  // We need to ignore the extra size at the start of the byte array because we are dealing with direct bytes
  override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
    compareRaw(b1, s1 + 4, l1 - 4, b2, s2 + 4, l2 - 4)

  def compare(w1: BytesWritable, w2: BytesWritable): Int =
    compareRaw(w1.getBytes, 0, w1.getLength, w2.getBytes, 0, w2.getLength)

  /** Util method */
  final def compareBytes(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
    WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2)
}
