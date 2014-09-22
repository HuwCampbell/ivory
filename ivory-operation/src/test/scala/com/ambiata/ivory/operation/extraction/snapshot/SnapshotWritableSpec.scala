package com.ambiata.ivory.operation.extraction.snapshot

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.mr.Writables
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable._
import org.specs2.execute.Result
import org.specs2.{ScalaCheck, Specification}

class SnapshotWritableSpec extends Specification with ScalaCheck { def is = s2"""

  Grouping                                            $grouping
  Sorting                                             $sorting
  Feature Id                                          $featureId
"""

  def grouping = prop((f1: FactAndPriority, f2: FactAndPriority) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new Grouping().compare(b1, 0, b1.length, b2, 0, b2.length) -> compareByGroup(f1.f, f3.f)
    }
  })

  def sorting = prop((f1: FactAndPriority, f2: FactAndPriority) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new Comparator().compare(b1, 0, b1.length, b2, 0, b2.length) -> compareAll(f1, f3)
    }
  })

  def featureId = prop((f1: FactAndPriority, i: Int) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f1.f, f1.p, bw, i)
    getFeatureId(bw) ==== i
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

  def compareByGroup(f1: Fact, f2: Fact): Int = {
    var e = f1.entity.compareTo(f2.entity)
    if (e == 0) {
      e = Math.abs(f1.featureId.hashCode).compareTo(Math.abs(f2.featureId.hashCode))
    }
    e
  }

  def compareAll(f1: FactAndPriority, f2: FactAndPriority): Int = {
    var e = compareByGroup(f1.f, f2.f)
    if (e == 0) {
      e = f1.f.date.int.compare(f2.f.date.int)
      if (e == 0) {
        e = f1.f.time.seconds.compare(f2.f.time.seconds)
        if (e == 0) {
          e = f1.p.toShort.compare(f2.p.toShort)
        }
      }
    }
    e
  }
}
