package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.Date
import com.ambiata.ivory.core.Fact
import spire.implicits._

/*
 * Note: These implement the less pure Reduction trait, because ReductionFoldWithDate requires a primitive
 *       type for the fact being examined (structs have one of their components specified and rolled in).
 *       Here, we don't mind or know what type to use, and one can look at intervals between events which
 *       come in as structs without trying to infer a type we would ignore anyway.
 */

/* The mean time between events. */
class IntervalMeanReducer(dates: DateOffsets) extends Reduction {
  var first: Boolean  = true
  var lastDate: Int   = 0
  var count: Long     = 0L
  var sum: Long       = 0L

  def clear(): Unit = {
    first     = true
    lastDate  = 0
    count     = 0L
    sum       = 0L
  }

  def update(fact: Fact): Unit = {
    if (!fact.isTombstone) {
      val x = dates.get(fact.date).value
      if (!first) {
        count += 1
        val y: Long = x - lastDate
        sum  = sum + y
      } else first = false
      lastDate = x
    }
  }

  def save: ThriftFactValue = {
    if (count == 0) ThriftFactValue.t(new ThriftTombstone())
    else ThriftFactValue.d(sum.toDouble() / count)
  }
}

/* The standard deviation of the interval. How sporadic the events are. */
class IntervalSDReducer(dates: DateOffsets) extends Reduction {
  var first: Boolean  = true
  var lastDate: Int   = 0
  var count: Long     = 0L
  var sum: Long       = 0L
  var squareSum: Long = 0L

  def clear(): Unit = {
    first     = true
    lastDate  = 0
    count     = 0L
    sum       = 0L
    squareSum = 0L
  }

  def update(fact: Fact): Unit = {
    if (!fact.isTombstone) {
      val x = dates.get(fact.date).value
      if (!first) {
        count += 1
        val y: Long = x - lastDate
        sum  = sum + y
        squareSum = squareSum + (y * y)
      } else first = false
      lastDate = x
    }
  }

  def save: ThriftFactValue =
    if (count < 2) ThriftFactValue.t(new ThriftTombstone())
    else {
      val mean = sum.toDouble() / count
      ThriftFactValue.d(((squareSum.toDouble() / count) - (mean * mean)).abs.sqrt())
    }
}

/* Gradient of the events, whether the frequency is increasing or decreasing over time */
class IntervalGradientReducer(dates: DateOffsets) extends Reduction {
  var first: Boolean = true
  var lastDate: Long = 0L
  var count: Long    = 0L
  var sumx: Long     = 0L
  var sumy: Long     = 0L
  var sumxx: Long    = 0L
  var sumxy: Long    = 0L

  def clear(): Unit = {
    first     = true
    lastDate  = 0L
    count     = 0L
    sumx      = 0L
    sumy      = 0L
    sumxx     = 0L
    sumxy     = 0L
  }

  def update(fact: Fact): Unit = {
    if (!fact.isTombstone) {
      val x = dates.get(fact.date).value.toLong
      if (!first) {
        count += 1
        val y = x - lastDate
        sumy = sumy + y
        sumx = sumx + x
        sumxx = sumxx + (x * x)
        sumxy = sumxy + (x * y)
      } else first = false
      lastDate = x
    }
  }

  def save: ThriftFactValue = {
    val z = (sumxx * count) - (sumx * sumx)
    if (z == 0 || count < 2) ThriftFactValue.t(new ThriftTombstone())
    else ThriftFactValue.d(((sumxy * count) - (sumx * sumy)).toDouble() / z.toDouble())
  }
}
