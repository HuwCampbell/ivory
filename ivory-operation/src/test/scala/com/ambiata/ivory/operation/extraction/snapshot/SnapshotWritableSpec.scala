package com.ambiata.ivory.operation.extraction.snapshot

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.ambiata.disorder.NaturalIntSmall
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.extraction.mode.ModeKey
import com.ambiata.ivory.operation.extraction.snapshot.SnapshotWritable._
import com.ambiata.mundane.bytes.Buffer
import com.ambiata.poacher.mr.Writables
import org.apache.hadoop.io.WritableComparator
import org.scalacheck._
import org.specs2.execute.Result
import org.specs2.{ScalaCheck, Specification}

class SnapshotWritableSpec extends Specification with ScalaCheck { def is = s2"""

  Grouping                                            $grouping
  Sorting                                             $sorting
  Feature Id                                          $featureId
  Entity                                              $entity
"""

  def grouping = prop((f1: FactAndPriorityKey, f2: FactAndPriorityKey) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new GroupingEntityFeatureId().compare(b1.bytes, b1.offset, b1.length, b2.bytes, b2.offset, b2.length) -> compareByGroup(f1.f, f3.f)
    }
  })

  def sorting = prop((f1: FactAndPriorityKey, f2: FactAndPriorityKey) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new Comparator().compare(b1.bytes, b1.offset, b1.length, b2.bytes, b2.offset, b2.length) -> compareAll(f1, f3)
    }
  })

  def featureId = prop((f1: FactAndPriorityKey, i: Int, key: Array[Byte]) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f1.f, f1.p, bw, FeatureIdIndex(i), ModeKey.blank)
    GroupingEntityFeatureId.getFeatureId(bw) ==== i
  })

  def entity = prop((f1: FactAndPriorityKey, i: Int, key: Array[Byte]) => {
    val bw = Writables.bytesWritable(4096)
    KeyState.set(f1.f, f1.p, bw, FeatureIdIndex(i), ModeKey.blank)
    GroupingEntityFeatureId.getEntity(bw) ==== f1.f.entity
  })

  def check(f1: FactAndPriorityKey, f2: FactAndPriorityKey)(f: (FactAndPriorityKey, Buffer, Buffer) => (Int, Int)): Result =
    seqToResult(List(
      "entity"   -> f1.copy(f = f1.f.withEntity(f2.f.entity)),
      "feature"  -> f1.copy(f = f1.f.withFeatureId(f2.f.featureId)),
      "date"     -> f1.copy(f = f1.f.withDate(f2.f.date)),
      "time"     -> f1.copy(f = f1.f.withTime(f2.f.time)),
      "priority" -> f1.copy(p = f2.p),
      "key"      -> f1.copy(k = f2.k),
      "equals"   -> f1,
      "diff"     -> f2
    ).map {
      case (message, f3) =>
        def norm(i: Int): Int = if (i == 0) 0 else if (i < 0) -1 else 1
        val (b1, b2) = set(f1, f3)
        val (a, b) = f(f3, b1, b2)
        (norm(a) ==== norm(b)).updateMessage(message + ": " + _)
    })

  def set(f1: FactAndPriorityKey, f2: FactAndPriorityKey): (Buffer, Buffer) = {
    val bw = Writables.bytesWritable(4096)
    def toBytes(f: FactAndPriorityKey): Buffer = {
      KeyState.set(f.f, f.p, bw, FeatureIdIndex(Math.abs(f.f.featureId.hashCode)), ModeKey.const(f.k))
      val b = new ByteArrayOutputStream()
      // Prepend some extra values to ensure offset is working
      (0 until f.o).foreach(b.write)
      // This appends the size to the array, which is what Hadoop does, so we do it too
      bw.write(new DataOutputStream(b))
      Buffer.wrapArray(b.toByteArray, f.o, bw.getLength + 4)
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

  def compareAll(f1: FactAndPriorityKey, f2: FactAndPriorityKey): Int = {
    var e = compareByGroup(f1.f, f2.f)
    if (e == 0) {
      e = f1.f.date.int.compare(f2.f.date.int)
      if (e == 0) {
        e = f1.f.time.seconds.compare(f2.f.time.seconds)
        if (e == 0) {
          e = WritableComparator.compareBytes(f1.k, 0, f1.k.length, f2.k, 0, f2.k.length)
          if (e == 0) {
            e = f1.p.toShort.compare(f2.p.toShort)
          }
        }
      }
    }
    e
  }

  case class FactAndPriorityKey(f: Fact, p: Priority, k: Array[Byte], o: Int)

  object FactAndPriorityKey {
    implicit def ArbitraryFactAndPriorityKey: Arbitrary[FactAndPriorityKey] =
      Arbitrary(for {
        f <- Arbitrary.arbitrary[Fact]
        p <- Arbitrary.arbitrary[Priority]
        o <- Gen.oneOf(Gen.const(0), Arbitrary.arbitrary[NaturalIntSmall].map(_.value))
        k <- Arbitrary.arbitrary[Array[Byte]]
      } yield new FactAndPriorityKey(f, p, k, o))
  }
}
