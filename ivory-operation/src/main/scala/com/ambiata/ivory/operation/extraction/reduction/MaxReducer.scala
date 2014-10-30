package com.ambiata.ivory.operation.extraction.reduction

import spire.math._
import spire.implicits._

case class MaxState[@specialized(Int, Long, Double) A](var max: A, var first: Boolean)

class MaxReducer[@specialized(Int, Long, Double) A](implicit N: Numeric[A]) extends ReductionFold[MaxState[A], A, A] {

  def initial: MaxState[A] =
    MaxState[A](0, true)

  def fold(a: MaxState[A], b: A): MaxState[A] = {
    if (b > a.max) a.max = b
    if (a.first == true) {
      a.max = b
      a.first = false
    }
    a
  }

  def tombstone(a: MaxState[A]): MaxState[A] =
    a

  def aggregate(a: MaxState[A]): A =
    a.max
}
