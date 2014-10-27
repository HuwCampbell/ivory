package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Date
import spire.math._
import spire.implicits._

case class GradientState[@specialized(Long, Double) A](var count: Long, var sumx: Long, var sumy: A, var sumxx: Long, var sumxy: A)

class GradientReducer[@specialized(Long, Double) A](dates: DateOffsets)(implicit N: Numeric[A]) extends ReductionFoldWithDate[GradientState[A], A, Double] {

  def initial: GradientState[A] =
    GradientState(0, 0, N.zero, 0, N.zero)

  def foldWithDate(s: GradientState[A], y: A, date: Date): GradientState[A] = {
    s.count += 1
    val x = N.toLong(dates.get(date).value)
    s.sumy = s.sumy + y
    s.sumx = s.sumx + x
    s.sumxx = s.sumxx + (x * x)
    s.sumxy = s.sumxy + (x * y)
    s
  }

  def tombstoneWithDate(a: GradientState[A], B: Date): GradientState[A] =
    a

  def aggregate(s: GradientState[A]): Double = {
    val count = s.count
    val z = (s.sumxx * count) - (s.sumx * s.sumx)
    if (z == 0) 0
    else ((s.sumxy * N.fromLong(count)) - (s.sumx * s.sumy)).toDouble() / z.toDouble()
  }
}
