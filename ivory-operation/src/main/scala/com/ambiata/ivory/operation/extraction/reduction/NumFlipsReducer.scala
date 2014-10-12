package com.ambiata.ivory.operation.extraction.reduction

case class NumFlipsState[@specialized(Int, Long, Double) A](var first: Boolean, var tombstone: Boolean, var count: Long, var last: A)

/**
 * Count the number of times a value changes during the window, where the first occurrence doesn't count.
 * The value of [[empty]] is only to avoid boxing (eg. using `null` or [[Option]]).
 */
class NumFlipsReducer[@specialized(Int, Long, Double) A](empty: A) extends ReductionFold[NumFlipsState[A], A, Long] {

  def initial: NumFlipsState[A] =
    NumFlipsState[A](true, false, 0, empty)

  def fold(a: NumFlipsState[A], b: A): NumFlipsState[A] = {
    if (a.first) {
      a.first = false
      a.last = b
    } else if(a.tombstone || a.last != b) {
      a.tombstone = false
      a.last = b
      a.count += 1
    }
    a
  }

  def tombstone(a: NumFlipsState[A]): NumFlipsState[A] = {
    if (a.first) {
      a.first = false
    } else if (!a.tombstone) {
      a.count += 1
    }
    a.tombstone = true
    a
  }

  def aggregate(a: NumFlipsState[A]): Long =
    a.count
}
