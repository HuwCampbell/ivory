package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}
import com.ambiata.ivory.core.thrift._
import spire.math._
import spire.implicits._

class IntervalReducersSpec extends Specification with ScalaCheck { def is = s2"""
  Interval mean reducer works with arbitrary facts       $meanInterval
  Interval sd reducer works with arbitrary facts         $sdInterval
  Interval grad reducer works with arbitrary facts       $gradInterval

"""

  def meanInterval = prop((facts: List[Fact]) => {
    val ds = facts.map(td => 0 -> td.date).sortBy(_._2)
    val dateOffsets = ReducerUtil.buildDateOffsets(ds)

    val r = new IntervalMeanReducer(dateOffsets)
    facts.sortBy(_.date).foreach(r.update)

    val filteredfacts = facts.filterNot(_.isTombstone).sortBy(_.date)
    val res = if (filteredfacts.length > 1)
        ThriftFactValue.d(filteredfacts.sliding(2, 1).collect { case a :: b :: Nil => DateTimeUtil.toDays(b.date) - DateTimeUtil.toDays(a.date) }.sum.toDouble / (filteredfacts.length - 1))
      else
        ThriftFactValue.t(new ThriftTombstone())
    r.save match {
      case tv if tv.isSetD => tv.getD must beCloseTo(res.getD, 4.significantFigures)
      case tv if tv.isSetT => res.isSetT must beTrue
      case x               => "Badline" ==== x.toString()
    }
  })

  def sdInterval = prop((facts: List[Fact]) => {
    val ds = facts.map(td => 0 -> td.date).sortBy(_._2)
    val dateOffsets = ReducerUtil.buildDateOffsets(ds)

    val r = new IntervalSDReducer(dateOffsets)
    facts.sortBy(_.date).foreach(r.update)

    val filteredfacts = facts.filterNot(_.isTombstone).sortBy(_.date)
    val res = if (filteredfacts.length > 2)
        ThriftFactValue.d(stdDev(filteredfacts.sliding(2, 1).collect { case a :: b :: Nil => DateTimeUtil.toDays(b.date) - DateTimeUtil.toDays(a.date) }.toList))
      else
        ThriftFactValue.t(new ThriftTombstone())
    r.save match {
      case tv if tv.isSetD => tv.getD must beCloseTo(res.getD, 4.significantFigures)
      case tv if tv.isSetT => res.isSetT must beTrue
      case x               => "Badline" ==== x.toString()
    }
  })

  def gradInterval = prop((facts: List[Fact]) => {
    val ds = facts.map(td => 0 -> td.date).sortBy(_._2)
    val dateOffsets = ReducerUtil.buildDateOffsets(ds)

    val r = new IntervalGradientReducer(dateOffsets)
    facts.sortBy(_.date).foreach(r.update)

    val filteredfacts = facts.filterNot(_.isTombstone).sortBy(_.date)
    val res = if (filteredfacts.length > 2)
        ThriftFactValue.d(gradient(filteredfacts.sliding(2, 1).collect { case a :: b :: Nil => (DateTimeUtil.toDays(b.date) - DateTimeUtil.toDays(a.date), b.date) }.toList))
      else
        ThriftFactValue.t(new ThriftTombstone())
    r.save match {
      case tv if tv.isSetD => tv.getD must beCloseTo(res.getD, 4.significantFigures)
      case tv if tv.isSetT => res.isSetT must beTrue
      case x               => false must beTrue
    }
  })

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
