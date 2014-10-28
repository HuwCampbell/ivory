package com.ambiata.ivory.operation.rename

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._, ArbitraryFacts._
import com.ambiata.ivory.operation.rename.RenameWritable._
import com.ambiata.poacher.mr.Writables
import org.apache.hadoop.io.WritableComparator
import org.specs2.execute.Result
import org.specs2.{ScalaCheck, Specification}

class RenameWritableSpec extends Specification with ScalaCheck { def is = s2"""

  Group by featureId and date                                     $groupFeatureDate
  Sorting by featureId and date, then entity, time and priority   $sortingFeatureDate
  Get featureId from group                                        $featureId
  Get date from group                                             $getDate
  Get entity hash from key                                        $getEntityHash
"""

  def groupFeatureDate = prop((f1: FactAndPriority, f2: FactAndPriority) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new GroupingByFeatureIdDate().compare(b1, 0, b1.length, b2, 0, b2.length) -> compareByFeatureDate(f1.f, f3.f)
    }
  })

  def sortingFeatureDate = prop((f1: FactAndPriority, f2: FactAndPriority) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new ComparatorFeatureIdDateEntityTimePriority().compare(b1, 0, b1.length, b2, 0, b2.length) -> compareByFeatureDateEntityTimePriority(f1, f3)
    }
  })

  def featureId = prop((f1: FactAndPriority, i: Int) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f1.f, f1.p, bw, i)
    GroupingByFeatureIdDate.getFeatureId(bw) ==== i
  })

  def getDate = prop((f1: FactAndPriority, i: Int) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f1.f, f1.p, bw, i)
    GroupingByFeatureIdDate.getDate(bw) ==== f1.f.date
  })

  def getEntityHash = prop((f1: FactAndPriority, i: Int) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f1.f, f1.p, bw, i)
    val bw2 = Writables.bytesWritable(4096)
    val e = f1.f.entity.getBytes("UTF-8")
    bw2.set(e, 0, e.length)
    GroupingByFeatureIdDate.getEntityHash(bw) ==== WritableComparator.hashBytes(e, 0, e.length)
  })

  def check(f1: FactAndPriority, f2: FactAndPriority)(f: (FactAndPriority, Array[Byte], Array[Byte]) => (Int, Int)): Result =
    seqToResult(List(
      "entity"   -> f1.copy(f = f1.f.withEntity(f2.f.entity)),
      "feature"  -> f1.copy(f = f1.f.withFeatureId(f2.f.featureId)),
      "date"     -> f1.copy(f = f1.f.withDate(f2.f.date)),
      "time"     -> f1.copy(f = f1.f.withTime(f2.f.time)),
      "priority" -> f1.copy(p = f2.p),
      "equals"   -> f1,
      "diff"     -> f2
    ).map {
      case (message, f3) =>
        def norm(i: Int): Int = if (i == 0) 0 else if (i < 0) -1 else 1
        val (b1, b2) = set(f1, f3)
        val (a, b) = f(f3, b1, b2)
        (norm(a) ==== norm(b)).updateMessage(message + ": " + _)
    })

  def set(f1: FactAndPriority, f2: FactAndPriority): (Array[Byte], Array[Byte]) = {
    val bw = Writables.bytesWritable(4096)
    def toBytes(f: FactAndPriority): Array[Byte] = {
      KeyState.set(f.f, f.p, bw, Math.abs(f.f.featureId.hashCode))
      val b = new ByteArrayOutputStream()
      // This appends the size to the array, which is what Hadoop does, so we do it too
      bw.write(new DataOutputStream(b))
      b.toByteArray
    }
    (toBytes(f1), toBytes(f2))
  }

  def compareByFeatureDate(f1: Fact, f2: Fact): Int = {
    val e = Math.abs(f1.featureId.hashCode).compareTo(Math.abs(f2.featureId.hashCode))
    if (e == 0) f1.date.int.compareTo(f2.date.int)
    else e
  }

  def compareByFeatureDateEntityTimePriority(f1: FactAndPriority, f2: FactAndPriority): Int = {
    var e = compareByFeatureDate(f1.f, f2.f)
    if (e == 0) {
      e = f1.f.entity.compare(f2.f.entity)
      if (e == 0) {
        e = f1.f.time.seconds.compare(f2.f.time.seconds)
        if (e == 0) {
          e =f1.p.toShort.compare(f2.p.toShort)
        }
      }
    }
    e
  }
}
