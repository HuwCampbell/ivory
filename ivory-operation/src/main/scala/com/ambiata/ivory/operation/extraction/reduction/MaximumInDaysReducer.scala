package com.ambiata.ivory.operation.extraction.reduction

class MaximumInDaysReducer extends DateReducer[Int] {

  def aggregate(dates: MutableDateSet): Int =
    dates.fold(0)((max, i) => Math.max(i, max))
}

class MaximumInWeeksReducer extends DateReducer[Int] {

  def aggregate(dates: MutableDateSet): Int =
    dates.foldWeeks(0)((max, i) => Math.max(i, max))
}
