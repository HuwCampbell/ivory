package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.{RawBytesComparator, ByteWriter}
import org.apache.hadoop.io.{BytesWritable, WritableComparator}
import org.apache.hadoop.mapreduce.Partitioner

/**
 * Utility classes for dealing with the key bytes in a snapshot.
 *
 * The byte layout is as follows, keeping in mind that entity is variable length,
 * but we know the size of everything after. The layout makes it easier to sort with a single byte array comparison.
 *
 * entity | featureId | date | time | priority
 *       -14         -10    -6     -2
 */
object SnapshotWritable {

  object Offsets {
    val featureId = 14
    val date = 10
    val time = 6
    val priority = 2
  }

  object KeyState {

    def set(f: Fact, priority: Priority, bw: BytesWritable, featureId: Int): Unit = {
      val bytes = bw.getBytes
      // We're assuming entity is never going to be greater than 4096
      val o1 = ByteWriter.writeStringUTF8(bytes, f.entity, 0)
      val end = o1 + Offsets.featureId
      // We don't need to bw.set() because we're sharing the array
      bw.setSize(end)
      ByteWriter.writeInt(bytes, featureId, end - Offsets.featureId)
      ByteWriter.writeInt(bytes, f.date.int, end - Offsets.date)
      ByteWriter.writeInt(bytes, f.time.seconds, end - Offsets.time)
      ByteWriter.writeShort(bytes, priority.toShort, end - Offsets.priority)
    }
  }

  class GroupingEntityFeatureId extends RawBytesComparator {
    def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int =
      compareBytes(b1, s1, l1 - Offsets.date, b2, s2, l2 - Offsets.date)
  }

  object GroupingEntityFeatureId {
    def getFeatureId(bw: BytesWritable): Int =
      WritableComparator.readInt(bw.getBytes, bw.getLength - Offsets.featureId)
  }

  class Comparator extends BytesWritable.Comparator

  class PartitionerEntityFeatureId extends Partitioner[BytesWritable, BytesWritable] {
    override def getPartition(k: BytesWritable, v: BytesWritable, partitions: Int): Int = {
      // Just partition based on entity+featureId
      (WritableComparator.hashBytes(k.getBytes, 0, k.getLength - Offsets.date) & Int.MaxValue) % partitions
    }
  }
}
