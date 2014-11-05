package com.ambiata.ivory.operation.extraction.squash

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._, ArbitraryFacts._
import com.ambiata.ivory.operation.extraction.squash.SquashWritable._
import com.ambiata.poacher.mr.Writables
import org.apache.hadoop.io.WritableComparator
import org.specs2.execute.Result
import org.specs2.{ScalaCheck, Specification}

class SquashWritableSpec extends Specification with ScalaCheck { def is = s2"""

  Group by featureId                                              $groupFeature
  Sorting by featureId then date, entity, time and priority       $sortingFeature
  Can get the entity hash                                         $entityHash
"""

  def groupFeature = prop((f1: Fact, f2: Fact) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new GroupingByFeatureId().compare(b1, 0, b1.length, b2, 0, b2.length) -> compareByFeature(f1, f3)
    }
  })

  def sortingFeature = prop((f1: Fact, f2: Fact) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new ComparatorFeatureId().compare(b1, 0, b1.length, b2, 0, b2.length) -> compareByFeatureEntityDateTimePriority(f1, f3)
    }
  })

  def entityHash = prop((f: Fact) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f, bw, 0)
    val bw2 = Writables.bytesWritable(4096)
    val e = f.entity.getBytes("UTF-8")
    bw2.set(e, 0, e.length)
    GroupingByFeatureId.hashEntity(bw) ==== WritableComparator.hashBytes(e, 0, e.length)
  })

  def check(f1: Fact, f2: Fact)(f: (Fact, Array[Byte], Array[Byte]) => (Int, Int)): Result =
    seqToResult(List(
      "entity"   -> f1.withEntity(f2.entity),
      "feature"  -> f1.withFeatureId(f2.featureId),
      "date"     -> f1.withDate(f2.date),
      "time"     -> f1.withTime(f2.time),
      "equals"   -> f1,
      "diff"     -> f2
    ).map {
      case (message, f3) =>
        def norm(i: Int): Int = if (i == 0) 0 else if (i < 0) -1 else 1
        val (b1, b2) = set(f1, f3)
        val (a, b) = f(f3, b1, b2)
        (norm(a) ==== norm(b)).updateMessage(message + ": " + _)
    })

  def set(f1: Fact, f2: Fact): (Array[Byte], Array[Byte]) = {
    val bw = Writables.bytesWritable(4096)
    def toBytes(f: Fact): Array[Byte] = {
      KeyState.set(f, bw, Math.abs(f.featureId.hashCode))
      val b = new ByteArrayOutputStream()
      // This appends the size to the array, which is what Hadoop does, so we do it too
      bw.write(new DataOutputStream(b))
      b.toByteArray
    }
    (toBytes(f1), toBytes(f2))
  }

  def compareByFeature(f1: Fact, f2: Fact): Int = {
    Math.abs(f1.featureId.hashCode).compareTo(Math.abs(f2.featureId.hashCode))
  }

  def compareByFeatureEntityDateTimePriority(f1: Fact, f2: Fact): Int = {
    var e = compareByFeature(f1, f2)
    if (e == 0) {
      e = f1.entity.compare(f2.entity)
      if (e == 0) {
        e = f1.datetime.long.compareTo(f2.datetime.long)
      }
    }
    e
  }
}
