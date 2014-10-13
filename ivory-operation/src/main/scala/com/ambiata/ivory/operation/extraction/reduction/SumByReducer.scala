package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, NotImplemented}
import spire.math._
import spire.implicits._

class SumByReducer[A](implicit N: Numeric[A]) extends ReductionFold2[KeyValue[String, A], String, A, KeyValue[String, A]] {

  NotImplemented.reducerPerformance("sum_by")

  def initial: KeyValue[String, A] =
    new KeyValue[String, A]

  def fold(a: KeyValue[String, A], key: String, b: A, date: Date): KeyValue[String, A] = {
    val old = a.getOrElse(key, N.zero)
    a.put(key, old + b)
    a
  }

  def tombstone(a: KeyValue[String, A], date: Date): KeyValue[String, A] =
    a

  def aggregate(a: KeyValue[String, A]): KeyValue[String, A] =
    a
}
