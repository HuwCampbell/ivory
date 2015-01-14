package com.ambiata.ivory.operation.extraction.reduction

class MinimumInDaysReducer extends DateReducer[Int] {

  def aggregate(set: MutableDateSet): Int =
    set.fold(0)(MinInDaysReducer.aggregate)
}

class MinimumInWeeksReducer extends DateReducer[Int] {

  def aggregate(set: MutableDateSet): Int =
    set.foldWeeks(0)(MinInDaysReducer.aggregate)
}

object MinInDaysReducer {

  def aggregate(min: Int, i: Int): Int =
    if (min == 0) i else if (i == 0) min else Math.min(i, min)
}
