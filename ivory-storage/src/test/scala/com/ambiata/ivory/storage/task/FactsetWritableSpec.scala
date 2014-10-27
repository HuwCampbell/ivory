package com.ambiata.ivory.storage.task

import java.io.{ByteArrayOutputStream, DataOutputStream}

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.Arbitraries._
import org.specs2.execute.Result
import org.specs2.{ScalaCheck, Specification}

class FactsetWritableSpec extends Specification with ScalaCheck { def is = s2"""

  Comparator                                          $comparator
  Feature Id                                          $featureId
  Date                                                $date
  Entity hash                                         $entityHash
"""

  def comparator = prop((f1: Fact, f2: Fact) => {
    check(f1, f2) { case (f3, b1, b2) =>
      new FactsetWritable.Comparator().compare(b1, 0, b1.length, b2, 0, b2.length) -> compare(f1, f3)
    }
  })

  def featureId = prop((f1: Fact, i: Int) => {
    val bw = FactsetWritable.create
    FactsetWritable.set(f1, bw, i)
    FactsetWritable.getFeatureId(bw) ==== i
  })

  def date = prop((f1: Fact, i: Int) => {
    val bw = FactsetWritable.create
    FactsetWritable.set(f1, bw, i)
    FactsetWritable.getDate(bw) ==== f1.date
  })

  def entityHash = prop((f1: Fact, i: Int) => {
    val bw = FactsetWritable.create
    FactsetWritable.set(f1, bw, i)
    FactsetWritable.getEntityHash(bw) ==== f1.entity.hashCode
  })

  def check(f1: Fact, f2: Fact)(f: (Fact, Array[Byte], Array[Byte]) => (Int, Int)): Result =
    seqToResult(List(
      "entity"   -> f1.withEntity(f2.entity),
      "feature"  -> f1.withFeatureId(f2.featureId),
      "date"     -> f1.withDate(f2.date),
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
    val bw = FactsetWritable.create
    def toBytes(f: Fact): Array[Byte] = {
      FactsetWritable.set(f, bw, getFeatureId(f))
      val b = new ByteArrayOutputStream()
      // This appends the size to the array, which is what Hadoop does, so we do it too
      bw.write(new DataOutputStream(b))
      b.toByteArray
    }
    (toBytes(f1), toBytes(f2))
  }

  def compare(f1: Fact, f2: Fact): Int = {
    val e = getFeatureId(f1).compareTo(getFeatureId(f2))
    if (e == 0) f1.date.int.compare(f2.date.int) else e
  }

  def getFeatureId(f: Fact): Int =
    Math.abs(f.featureId.hashCode)
}
