package com.ambiata.ivory.storage.task

import com.ambiata.ivory.core._
import com.ambiata.poacher.mr.{Writables, RawBytesComparator, ByteWriter}
import org.apache.hadoop.io.{WritableComparator, BytesWritable}

/**
 * Utility classes for dealing with the key bytes in ingest/recreate.
 *
 * The byte layout is as follows.
 *
 * featureId | date | entityHash
 *           4      8            12
 */
object FactsetWritable {

  def create: BytesWritable =
    Writables.bytesWritable(12)

  def set(f: Fact, bw: BytesWritable, featureId: FeatureIdIndex): Unit = {
    val bytes = bw.getBytes
    ByteWriter.writeInt(bytes, featureId.int, 0)
    ByteWriter.writeInt(bytes, f.date.underlying, 4)
    ByteWriter.writeInt(bytes, f.entity.hashCode, 8)
    bw.setSize(12)
  }

  def getFeatureId(bw: BytesWritable): FeatureIdIndex =
    FeatureIdIndex(WritableComparator.readInt(bw.getBytes, 0))

  def getDate(bw: BytesWritable): Date =
    Date.unsafeFromInt(WritableComparator.readInt(bw.getBytes, 4))

  def getEntityHash(bw: BytesWritable): Int =
    WritableComparator.readInt(bw.getBytes, 8)

  class Comparator extends RawBytesComparator {
    def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
      // Ignore the entity hash
      compareBytes(b1, s1, l1 - 4, b2, s2, l2 - 4)
  }
}
