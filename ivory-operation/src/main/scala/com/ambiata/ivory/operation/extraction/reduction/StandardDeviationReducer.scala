package com.ambiata.ivory.operation.extraction.reduction

import spire.math._
import spire.implicits._

case class StandardDeviationState[@specialized(Long, Double) A](var count: Long, var sum: A, var squareSum: A)

class StandardDeviationReducer[@specialized(Long, Double) A](implicit N: Numeric[A]) extends ReductionFold[StandardDeviationState[A], A, Double] {

  def initial: StandardDeviationState[A] =
    StandardDeviationState(0, N.zero, N.zero)

  def fold(s: StandardDeviationState[A], d: A): StandardDeviationState[A] = {
    s.count += 1
    s.sum = s.sum + d
    s.squareSum = s.squareSum + (d * d)
    s
  }

  def tombstone(s: StandardDeviationState[A]): StandardDeviationState[A] =
    s

  def aggregate(s: StandardDeviationState[A]): Double =
    if (s.count < 2) 0.0
    else {
      val mean = s.sum.toDouble() / s.count
      ((s.squareSum.toDouble() / s.count) - (mean * mean)).abs.sqrt()
    }
}
