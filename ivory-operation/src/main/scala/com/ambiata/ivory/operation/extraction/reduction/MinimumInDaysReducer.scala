package com.ambiata.ivory.operation.extraction.reduction

class MinimumInDaysReducer extends DateReducer[Int] {

  def aggregate(set: MutableDateSet): Int =
    set.fold(Int.MaxValue)((min, i) => if (i != 0) Math.min(i, min) else min)
}

class MinimumInWeeksReducer extends DateReducer[Int] {

  def aggregate(set: MutableDateSet): Int =
    set.foldWeeks(Int.MaxValue)((min, i) => if (i != 0) Math.min(i, min) else min)
}
