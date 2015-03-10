package com.ambiata.ivory.operation.extraction.snapshot

import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.{Emitter, EmitterThrift}
import com.ambiata.ivory.operation.extraction.mode.ModeKey
import com.ambiata.mundane.bytes.Buffer
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{BytesWritable, WritableComparator}
import org.apache.hadoop.mapreduce.Partitioner

/**
 * Utility classes for dealing with the key bytes in a snapshot.
 *
 * The byte layout is as follows, keeping in mind that both entity and key are variable length.
 * The length of the key is kept at the end is make it easier to sort the bytes directly.
 *
 * entity | featureId | date | time | key | priority | key_length
 *   ?          4        4      4      ?       2          4
 *
 *                      !!! WARNING !!!
 * This is used for the chord mr job also, so be aware when changing
 */
object SnapshotWritable {

  object Offsets {

    def keyLength(b: Array[Byte], s: Int, l: Int): Int =
      WritableComparator.readInt(b, s + l - 4)

    def featureId(b: Array[Byte], s: Int, l: Int): Int =
      l - keyLength(b, s, l) - 18

    def date(b: Array[Byte], s: Int, l: Int): Int =
      featureId(b, s, l) + 4

    def key(b: Array[Byte], s: Int, l: Int): Int =
      featureId(b, s, l) + 12
  }

  def writeAndEmit(fact: MutableFact, priority: Priority, featureIdIndex: FeatureIdIndex, modeKeys: Array[ModeKey],
                   kout: BytesWritable, vout: BytesWritable, serializer: ThriftSerialiser,
                   emitter: Emitter[BytesWritable, BytesWritable]): Unit = {
    KeyState.set(fact, priority, kout, featureIdIndex, modeKeys(featureIdIndex.int))
    EmitterThrift.writeAndEmit(fact, kout, vout, serializer, emitter)
  }

  object KeyState {

    def set(f: Fact, priority: Priority, bw: BytesWritable, featureId: FeatureIdIndex, modeKey: ModeKey): Unit = {
      val bytes = bw.getBytes
      // We're assuming entity is never going to be greater than 4096
      val o1 = ByteWriter.writeStringUTF8(bytes, f.entity, 0)
      ByteWriter.writeInt(bytes, featureId.int, o1)
      ByteWriter.writeInt(bytes, f.date.int, o1 + 4)
      ByteWriter.writeInt(bytes, f.time.seconds, o1 + 8)
      val key = modeKey.append(f, Buffer.wrapArray(bytes, o1 + 12, 0))
      // FIX We should be passing around Buffer instead of BytesWritable
      if (bytes != key.bytes)
        Crash.error(Crash.DataIntegrity, s"Key from entity '${f.entity}' was too large")
      ByteWriter.writeShort(bytes, priority.toShort, o1 + key.length + 12)
      ByteWriter.writeInt(bytes, key.length, o1 + key.length + 14)
      // We don't need to bw.set() because we're sharing the array
      bw.setSize(o1 + key.length + 18)
    }
  }

  class GroupingEntityFeatureId extends RawBytesComparator {
    def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
      compareBytes(b1, s1, Offsets.date(b1, s1, l1), b2, s2, Offsets.date(b2, s2, l2))
    }
  }

  object GroupingEntityFeatureId {
    def getFeatureId(bw: BytesWritable): Int =
      WritableComparator.readInt(bw.getBytes, Offsets.featureId(bw.getBytes, 0, bw.getLength))
    def getEntity(bw: BytesWritable): String =
      new String(bw.getBytes, 0, Offsets.featureId(bw.getBytes, 0, bw.getLength), "UTF-8")
  }

  class Comparator extends RawBytesComparator {
    def compareRaw(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
      val kl1 = Offsets.keyLength(b1, s1, l1)
      val kl2 = Offsets.keyLength(b2, s2, l2)
      val k1 = l1 - 6 - kl1
      val k2 = l2 - 6 - kl2
      var i = compareBytes(b1, s1, k1, b2, s2, k2)
      if (i == 0) {
        i = compareBytes(b1, s1 + k1, kl1, b2, s2 + k2, kl2)
        if (i == 0) {
          i = compareBytes(b1, s1 + l1 - 6, 2, b2, s2 + l2 - 6, 2)
        }
      }
      i
    }
  }

  class PartitionerEntityFeatureId extends Partitioner[BytesWritable, BytesWritable] {
    override def getPartition(k: BytesWritable, v: BytesWritable, partitions: Int): Int = {
      // Just partition based on entity+featureId
      (WritableComparator.hashBytes(k.getBytes, 0, Offsets.date(k.getBytes, 0, k.getLength)) & Int.MaxValue) % partitions
    }
  }
}
