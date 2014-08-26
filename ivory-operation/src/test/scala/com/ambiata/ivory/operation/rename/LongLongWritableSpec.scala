package com.ambiata.ivory.operation.rename

import java.io._

import org.scalacheck.Arbitrary, Arbitrary._
import org.specs2.{ScalaCheck, Specification}

class LongLongWritableSpec extends Specification with ScalaCheck { def is = s2"""
LongLongWritable
================
  writing and reading two values is symmetrical       $writeRead
  compare with first values that are different        $compareFirst
  compare with second values that are different       $compareSecond
  compare with values are equal                       $compareEquals
  hashCode is distributed evenly                      $hashCodeDistribution
"""

  def llw(l1: Long, l2: Long) = new LongLongWritable(l1, l2)

  implicit def ArbitraryLLW: Arbitrary[LongLongWritable] = Arbitrary(for {
    l1 <- arbitrary[Long]
    l2 <- arbitrary[Long]
  } yield llw(l1, l2))

  def writeRead = prop((in: LongLongWritable, blank: LongLongWritable) => {
    val stream = new ByteArrayOutputStream()
    in.write(new DataOutputStream(stream))
    blank.readFields(new DataInputStream(new ByteArrayInputStream(stream.toByteArray)))
    blank.l1 ==== in.l1 and blank.l2 ==== in.l2
  })

  def compareFirst = prop((l1: Long, l2: Long, any: Long) => l1 != l2 ==> {
    val (ls, ll) = if (l1 < l2) (l1, l2) else (l2, l1)
    llw(ls, any).compareTo(llw(ll, any)) ==== -1 and llw(ll, any).compareTo(llw(ls, any)) ==== 1
  })

  def compareSecond = prop((l1: Long, l2: Long, any: Long) => l1 != l2 ==> {
    val (ls, ll) = if (l1 < l2) (l1, l2) else (l2, l1)
    llw(any, ls).compareTo(llw(any, ll)) ==== -1 and llw(any, ll).compareTo(llw(any, ls)) ==== 1
  })

  def compareEquals = prop((w: LongLongWritable) => {
    w.compareTo(w) ==== 0
  })

  def hashCodeDistribution = prop((w: List[LongLongWritable]) => w.size > 4 ==> {
    w.map(_.hashCode()).foldLeft(new Array[Int](32)) {
      case (a, h) =>
        var n = h
        (0 until 32).foreach { i =>
          a(i) += (n & 1)
          n = n >>> 1
        }
        a
    }.count(_ > 0) must beGreaterThanOrEqualTo(w.length / 2)
  }).set(maxSize = 31)
}
