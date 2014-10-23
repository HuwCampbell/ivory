package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.task.BaseFactsPartitioner
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{BytesWritable, WritableComparator}

/**
 * Utility classes for dealing with the key bytes in a rename.
 *
 * The byte layout is as follows, keeping in mind that entity is variable length,
 * but we know the size of everything before and after.
 * The layout makes it easier to sort with a single byte array comparison.
 *
 * featureId | date | entity | time | priority
 *           4      8        -6     -2
 */
object RenameWritable {

  object Offsets {
    object Before {
      val featureId = 0
      val date = 4
      val entity = 8
    }
    object After {
      val time = 6
      val priority = 2
    }
  }

  object KeyState {

    def set(f: Fact, priority: Priority, bw: BytesWritable, featureId: Int): Unit = {
      val bytes = bw.getBytes
      ByteWriter.writeInt(bytes, featureId, Offsets.Before.featureId)
      ByteWriter.writeInt(bytes, f.date.int, Offsets.Before.date)
      // We're assuming entity is never going to be greater than 4096
      val o1 = ByteWriter.writeStringUTF8(bytes, f.entity, Offsets.Before.entity)
      val end = o1 + Offsets.After.time
      ByteWriter.writeInt(bytes, f.time.seconds, end - Offsets.After.time)
      ByteWriter.writeShort(bytes, priority.toShort, end - Offsets.After.priority)
      // We don't need to bw.set() because we're sharing the array
      bw.setSize(end)
    }
  }

  object GroupingByFeatureIdDate {

    def getFeatureId(bw: BytesWritable): Int =
      WritableComparator.readInt(bw.getBytes, Offsets.Before.featureId)

    def getDate(bw: BytesWritable): Date =
      Date.unsafeFromInt(WritableComparator.readInt(bw.getBytes, Offsets.Before.date))
  }

  class ComparatorFeatureIdDateEntityTimePriority extends BytesWritable.Comparator

  class GroupingByFeatureIdDate extends RawBytesComparator {
    def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
      compareBytes(b1, s1, Offsets.Before.entity, b2, s2, Offsets.Before.entity)
  }

  /** We only partition by featureId to avoid skew */
  class PartitionerFeatureId extends BaseFactsPartitioner[BytesWritable] {
    def getFeatureId(bw: BytesWritable): Int =
      GroupingByFeatureIdDate.getFeatureId(bw)
  }
}
