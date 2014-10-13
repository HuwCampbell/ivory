package com.ambiata.ivory.operation.extraction.reduction

object QuantileInReducer {

  def aggregate(k: Int, q: Int, dates: MutableDateSet, sorted: Array[Int], bucket: Int): Double = {
    dates.sortBuckets(sorted, bucket)
    quantileOnSorted(k, q, sorted)
  }

  def quantileOnSorted(k: Int, q: Int, sorted: Array[Int]): Double = {
    val actual = k.toDouble / q * (sorted.length - 1)
    val ceil  = Math.ceil(actual).toInt
    val floor = Math.floor(actual).toInt
    val floorweight = ceil - actual

    sorted(floor).toDouble * floorweight + sorted(ceil).toDouble * (1.0 - floorweight)
  }
}

class QuantileInDaysReducer(offsets: DateOffsets, k: Int, q: Int) extends DateReducer[Double] {

  // Only allocate a single array for sorting the dates
  val sorted = new Array[Int](offsets.count)

  def aggregate(dates: MutableDateSet): Double =
    QuantileInReducer.aggregate(k, q, dates, sorted, 1)
}

class QuantileInWeeksReducer(offsets: DateOffsets, k: Int, q: Int) extends DateReducer[Double] {

  // Only allocate a single array for sorting the dates
  val sorted = new Array[Int](offsets.count / 7)

  def aggregate(dates: MutableDateSet): Double =
    QuantileInReducer.aggregate(k, q, dates, sorted, 7)
}
