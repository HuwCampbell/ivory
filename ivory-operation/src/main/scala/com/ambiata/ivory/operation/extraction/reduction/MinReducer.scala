package com.ambiata.ivory.operation.extraction.reduction

import spire.math._
import spire.implicits._

case class MinState[@specialized(Int, Long, Double) A](var min: A, var first: Boolean)

class MinReducer[@specialized(Int, Long, Double) A](implicit N: Numeric[A]) extends ReductionFold[MinState[A], A, A] {

  def initial: MinState[A] =
    MinState[A](0, true)

  def fold(a: MinState[A], b: A): MinState[A] = {
    if (b < a.min) a.min = b
    if (a.first == true) {
      a.min = b
      a.first = false
    }
    a
  }

  def tombstone(a: MinState[A]): MinState[A] =
    a

  def aggregate(a: MinState[A]): A =
    a.min
}
