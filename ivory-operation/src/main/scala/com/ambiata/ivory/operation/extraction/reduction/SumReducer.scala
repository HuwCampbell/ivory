package com.ambiata.ivory.operation.extraction.reduction

import spire.math._
import spire.implicits._

// It's safer for specialization if we wrap the single value
case class SumState[@specialized(Long, Double) A](var sum: A)

class SumReducer[@specialized(Long, Double) A](implicit N: Numeric[A]) extends ReductionFold[SumState[A], A, A] {

  def initial: SumState[A] =
    SumState(N.zero)

  def fold(a: SumState[A], b: A): SumState[A] = {
    a.sum += b
    a
  }

  def tombstone(a: SumState[A]): SumState[A] =
    a

  def aggregate(a: SumState[A]): A =
    a.sum
}
