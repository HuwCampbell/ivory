package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher._
import org.scalacheck.{Gen, Arbitrary}
import spire.math._
import spire.implicits._

class GradientReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the gradient of an arbitrary number of doubles            $gradientDouble
  Take the gradient of an arbitrary number of ints               $gradientInt
  Gradient reducer laws                                          $gradientLaws
"""

  case class SaneDouble(d: Double)
  implicit def SaneDoubleArbitrary: Arbitrary[SaneDouble] =
    Arbitrary(Gen.choose(-10000.0, 10000.0).map(SaneDouble.apply))

  def gradientDouble = prop((xs: ValuesWithDate[SaneDouble]) =>
    gradient(xs.map(_.d))
  )

  def gradientInt = prop((xs: ValuesWithDate[Int]) =>
    gradient(xs.map(_.toLong))
  )

  def gradient[A: Numeric](xs: ValuesWithDate[A]): MatchResult[Double] = {
    ReducerUtil.runWithDates(new GradientReducer[A](xs.offsets), xs.ds) must beCloseTo(
      ReducerMathsHelpers.gradient(xs.ds)
    , 5.significantFigures)
  }

  def gradientLaws =
    ReducerUtil.reductionFoldWithDateLaws(offsets => new GradientReducer[Int](offsets))
}

object ReducerMathsHelpers {

  def stdDev[A: Numeric](ds: List[A]): Double = {
    if (ds.length < 1) 0.0 else {
      val mean   = ds.map(_.toDouble()).sum / ds.length
      val moment = ds.map(_.toDouble()).map(n => Math.pow(n - mean, 2)).sum / ds.length
      Math.sqrt(moment)
    }
  }

  def gradient[A: Numeric](xs: List[(A, Date)]): Double = {
    if (xs.length < 2) 0.0 else {
      val meanValue = xs.map(_._1.toDouble()).sum / xs.length
      val meanDate  = xs.map(x => DateTimeUtil.toDays(x._2).toDouble).sum / xs.length
      val moment    = xs.map(x => (x._1.toDouble() - meanValue) * (DateTimeUtil.toDays(x._2) - meanDate)).sum
      val square    = xs.map(x => DateTimeUtil.toDays(x._2).toDouble - meanDate).map(x => x * x).sum
      moment.toDouble() / square
    }
  }
}
