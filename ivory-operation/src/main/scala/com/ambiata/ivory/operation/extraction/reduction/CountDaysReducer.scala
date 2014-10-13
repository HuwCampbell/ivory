package com.ambiata.ivory.operation.extraction.reduction

class CountDaysReducer extends DateReducer[Int] {

  def aggregate(set: MutableDateSet): Int =
    set.countUnique
}
