package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.{RawBytesComparator, ByteWriter}
import org.apache.hadoop.io.{BytesWritable, WritableComparator}

/**
 * Utility classes for dealing with the key bytes in a squash.
 *
 * The byte layout is as follows, keeping in mind that entity is variable length,
 * but we know the size of everything before and after.
 * The layout makes it easier to sort with a single byte array comparison.
 *
 * featureId | entity | date | time
 *           4       -8    -4
 */
object SquashWritable {

  object Offsets {
    object Before {
      val featureId = 0
      val date = 8
      val time = 4
    }
    object After {
      val featureId = 4
    }
  }

  object KeyState {

    def set(f: Fact, bw: BytesWritable, featureId: Int): Unit = {
      val bytes = bw.getBytes
      ByteWriter.writeInt(bytes, featureId, Offsets.Before.featureId)
      // We're assuming entity is never going to be greater than 4096
      val o1 = ByteWriter.writeStringUTF8(bytes, f.entity, Offsets.After.featureId)
      val end = o1 + Offsets.Before.date
      // We don't need to bw.set() because we're sharing the array
      bw.setSize(end)
      ByteWriter.writeInt(bytes, f.date.int, end - Offsets.Before.date)
      ByteWriter.writeInt(bytes, f.time.seconds, end - Offsets.Before.time)
    }
  }

  class GroupingByFeatureId extends RawBytesComparator {
    def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
      compareBytes(b1, s1, 4, b2, s2, 4)
  }

  object GroupingByFeatureId {

    def getFeatureId(bw: BytesWritable): Int =
      WritableComparator.readInt(bw.getBytes, Offsets.Before.featureId)

    def hashEntity(bw: BytesWritable): Int =
      WritableComparator.hashBytes(bw.getBytes, Offsets.After.featureId, bw.getLength - (Offsets.Before.date + Offsets.After.featureId))
  }

  class ComparatorFeatureId extends BytesWritable.Comparator
}
