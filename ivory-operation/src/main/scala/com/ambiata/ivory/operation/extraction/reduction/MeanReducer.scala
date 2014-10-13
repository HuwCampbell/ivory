package com.ambiata.ivory.operation.extraction.reduction

import spire.math._
import spire.implicits._

case class MeanState[@specialized(Long, Double) A](var count: Long, var sum: A)

class MeanReducer[@specialized(Long, Double) A](implicit N: Numeric[A]) extends ReductionFold[MeanState[A], A, Double] {

  def initial: MeanState[A] =
    MeanState[A](0, N.zero)

  def fold(a: MeanState[A], b: A): MeanState[A] = {
    a.count += 1
    a.sum = a.sum + b
    a
  }

  def tombstone(a: MeanState[A]): MeanState[A] =
    a

  def aggregate(a: MeanState[A]): Double =
    if (a.count == 0) 0
    else a.sum.toDouble() / a.count
}
