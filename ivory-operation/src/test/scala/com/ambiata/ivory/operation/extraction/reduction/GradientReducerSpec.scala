package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Date
import com.ambiata.ivory.operation.extraction.reduction.ReductionArbitraries._
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher._
import org.scalacheck.{Gen, Arbitrary}
import spire.math._
import spire.implicits._

class GradientReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the gradient of an arbitrary number of doubles            $gradientDouble
  Take the gradient of an arbitrary number of ints               $gradientInt
"""

  case class SaneDouble(d: Double)
  implicit def SaneDoubleArbitrary: Arbitrary[SaneDouble] =
    Arbitrary(Gen.choose(-10000.0, 10000.0).map(SaneDouble.apply))

  def gradientDouble = prop((xs: List[(SaneDouble, TestDate)]) =>
    gradient(xs.map(td => td._1.d -> td._2))
  )

  def gradientInt = prop((xs: List[(Int, TestDate)]) =>
    gradient(xs.map(td => td._1.toLong -> td._2))
  )

  def gradient[A: Numeric](xs: List[(A, TestDate)]): MatchResult[Double] = {
    val ds = xs.map(td => td._1 -> td._2.d).sortBy(_._2)
    val dateOffsets = ReducerUtil.buildDateOffsets(ds)
    ReducerUtil.runWithDates(new GradientReducer[A](dateOffsets), ds) must beCloseTo(
      ReducerMathsHelpers.gradient(ds)
    , 2.significantFigures)
  }
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
      val meanDate  = xs.map(x => DateTimeUtil.toDays(x._2).toLong).sum / xs.length
      val moment    = xs.map(x => (x._1.toDouble() - meanValue) * (DateTimeUtil.toDays(x._2) - meanDate)).sum
      val square    = xs.map(x => DateTimeUtil.toDays(x._2).toLong - meanDate).map(x => x * x).sum
      moment.toDouble() / square
    }
  }
}
