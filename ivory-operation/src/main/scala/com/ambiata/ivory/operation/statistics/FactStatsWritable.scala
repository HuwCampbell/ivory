package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{BytesWritable, WritableComparator}

/**
 * Utility classes for dealing with the key bytes in fact stats.
 *
 */
object FactStatsWritable {
  sealed trait StatsType {
    val byte: Byte
  }
  case object Numerical extends StatsType {
    val byte = 0.toByte
  }
  case object Categorical extends StatsType {
    val byte = 1.toByte
  }

  object KeyState {
    object Offsets {
      val featureId = 0
      val date = 4
      val ty = 8
    }

    def set(featureId: Int, date: Date, ty: StatsType, bw: BytesWritable): Unit = {
      val bytes = bw.getBytes
      ByteWriter.writeInt(bytes, featureId, Offsets.featureId)
      ByteWriter.writeInt(bytes, date.int, Offsets.date)
      ByteWriter.writeByte(bytes, ty.byte, Offsets.ty)
      bw.setSize(9)
    }

    def getFeatureId(bw: BytesWritable): Int =
      WritableComparator.readInt(bw.getBytes, Offsets.featureId)

    def getDate(bw: BytesWritable): Date =
      Date.unsafeFromInt(WritableComparator.readInt(bw.getBytes, Offsets.date))

    def getType(bw: BytesWritable): StatsType =
      bw.getBytes()(Offsets.ty) match {
        case Numerical.byte   => Numerical
        case Categorical.byte => Categorical
      }
  }
}
