package com.ambiata.ivory.operation.extraction.reduction

object MeanInReducer {

  def aggregate(dates: MutableDateSet, size: Int): Double = {
    var sum = 0
    var count = 0
    dates.foreachBuckets(size) { i =>
      sum += i
      count += 1
    }
    if (count == 0) 0 else sum / count
  }
}

class MeanInDaysReducer extends DateReducer[Double] {

  def aggregate(dates: MutableDateSet): Double =
    MeanInReducer.aggregate(dates, 1)
}

class MeanInWeeksReducer extends DateReducer[Double] {

  def aggregate(dates: MutableDateSet): Double =
    MeanInReducer.aggregate(dates, 7)
}
