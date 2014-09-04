package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.ByteWriter
import org.apache.hadoop.io.{BytesWritable, WritableComparator}
import org.apache.hadoop.mapreduce.Partitioner

/**
 * Utility classes for dealing with the key bytes in a snapshot.
 *
 * The byte layout is as follows, keeping in mind that namespace and the feature is variable length,
 * but we know the size of everything after. The layout makes it easier to sort with a single byte array comparison.
 *
 * namespace + feature | date | time | priority
 *                    -10    -6     -2
 */
object SnapshotWritable {

  object Offsets {
    val date = 10
    val time = 6
    val priority = 2
  }

  object KeyState {

    def set(f: Fact, priority: Priority, bw: BytesWritable) {
      val bytes = bw.getBytes
      val o1 = ByteWriter.writeStringUTF8(bytes, f.entity, 0)
      val o2 = ByteWriter.writeStringUTF8(bytes, f.namespaceUnsafe.name, o1)
      val o3 = ByteWriter.writeStringUTF8(bytes, f.feature, o2)
      val end = o3 + Offsets.date
      ByteWriter.writeInt(bytes, f.date.int, end - Offsets.date)
      ByteWriter.writeInt(bytes, f.time.seconds, end - Offsets.time)
      ByteWriter.writeShort(bytes, priority.toShort, end - Offsets.priority)
      bw.set(bytes, 0, end)
    }
  }

  class Grouping extends WritableComparator(classOf[BytesWritable], false) {
    override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
      // We need to ignore the extra size at the start of the byte array because we are dealing with direct bytes
      WritableComparator.compareBytes(b1, s1 + 4, l1 - Offsets.date - 4, b2, s2 + 4, l1 - Offsets.date - 4)
    }
  }

  class Comparator extends BytesWritable.Comparator

  class SPartitioner extends Partitioner[BytesWritable, BytesWritable] {
    override def getPartition(k: BytesWritable, v: BytesWritable, partitions: Int): Int = {
      // Just partition based on the group
      WritableComparator.hashBytes(k.getBytes, 0, v.getLength - Offsets.date) % partitions
    }
  }
}
