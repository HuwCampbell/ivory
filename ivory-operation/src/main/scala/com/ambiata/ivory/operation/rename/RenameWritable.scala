package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.ByteWriter
import com.ambiata.ivory.storage.task.BaseFactsPartitioner
import org.apache.hadoop.io.{BytesWritable, RawComparator, WritableComparator}
import org.apache.hadoop.mapreduce.Partitioner

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

  class GroupingByFeatureIdDate extends RawComparator[BytesWritable] {
    override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
      // We need to ignore the extra size at the start of the byte array because we are dealing with direct bytes
      WritableComparator.compareBytes(
        b1, s1 + 4, Offsets.Before.entity,
        b2, s2 + 4, Offsets.Before.entity
      )

    def compare(w1: BytesWritable, w2: BytesWritable): Int =
      sys.error("Not implemented")
  }

  /** We only partition by featureId to avoid skew */
  class PartitionerFeatureId extends BaseFactsPartitioner[BytesWritable] {
    def getFeatureId(bw: BytesWritable): Int =
      GroupingByFeatureIdDate.getFeatureId(bw)
  }
}
