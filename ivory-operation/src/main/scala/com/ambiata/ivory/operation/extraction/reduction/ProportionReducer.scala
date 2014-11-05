package com.ambiata.ivory.operation.extraction.reduction

class ProportionState(var count: Long, var total: Long)

class ProportionReducer[A](value: A) extends ReductionFold[ProportionState, A, Double] {

  def initial: ProportionState =
    new ProportionState(0, 0)

  def fold(a: ProportionState, b: A): ProportionState = {
    if (value == b) a.count += 1
    a.total += 1
    a
  }

  def tombstone(a: ProportionState): ProportionState = {
    a.total += 1
    a
  }

  def aggregate(a: ProportionState): Double =
    if (a.total == 0) 0
    else a.count.toDouble / a.total
}
