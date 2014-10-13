package com.ambiata.ivory.operation.extraction.reduction

import org.scalacheck.{Arbitrary, Gen}
import org.specs2.matcher.MatchResult
import org.specs2.{ScalaCheck, Specification}
import SignificantFigures._
import spire.math._
import spire.implicits._

class StdReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the standard deviation of a known special case                      $stdKnown
  Take the standard deviation of an arbitrary number of doubles            $stdDouble
  Take the standard deviation of an arbitrary number of ints               $stdInt
"""
  case class SaneDouble(d: Double)
  implicit def SaneDoubleArbitrary: Arbitrary[SaneDouble] =
    Arbitrary(Gen.choose(-100000.0, 100000.0).map(SaneDouble.apply))

  case class SaneInt(i: Int)
  implicit def SaneIntArbitrary: Arbitrary[SaneInt] =
    Arbitrary(Gen.choose(-100000, 100000).map(SaneInt.apply))

  def stdKnown =
    ReducerUtil.run(new StandardDeviationReducer[Long](), List(1L, 9L)) ==== 4.0

  def stdDouble = prop((xs: List[SaneDouble]) =>
    stdDev(xs.map(_.d))
  )

  def stdInt = prop((xs: List[SaneInt]) =>
    stdDev(xs.map(_.i.toLong))
  )

  def stdDev[A: Numeric](ds: List[A]): MatchResult[Double] = {
    ReducerUtil.run(new StandardDeviationReducer[A](), ds) must SignificantFigures.beCloseTo(
      if (ds.length < 1) 0.0 else {
        val mean   = ds.map(_.toDouble()).sum / ds.length
        val moment = ds.map(_.toDouble()).map(n => Math.pow(n - mean, 2)).sum / ds.length
        Math.sqrt(moment)
      }, 7.significantfigures)
  }
}
