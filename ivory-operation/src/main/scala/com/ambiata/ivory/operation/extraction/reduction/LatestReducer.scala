package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Fact
import com.ambiata.ivory.core.thrift._

class LatestReducer extends Reduction {

  var value: ThriftFactValue = null
  var tombstone = true

  def clear(): Unit = {
    value = null
    tombstone = true
  }

  def update(fv: Fact): Unit = {
    // It's safe to keep the instance here because (currently) on read() new inner-structs are created each time
    value = fv.toThrift.getValue
    tombstone = fv.isTombstone
  }

  def skip(f: Fact, reason: String): Unit = ()

  def save: ThriftFactValue =
    if (!tombstone) value else null
}

/** Handle the latest of a single struct value */
class LatestStructReducer[@specialized(Int, Float, Double, Boolean) A](seed: A) extends ReductionFold[ValueOrTombstone[A], A, ValueOrTombstone[A]] {

  val start = ValueOrTombstone[A](seed, true)

  def initial: ValueOrTombstone[A] = {
    start.tombstone = true
    start
  }

  def fold(a: ValueOrTombstone[A], value: A): ValueOrTombstone[A] = {
    a.value = value
    a.tombstone = false
    a
  }

  def tombstone(a: ValueOrTombstone[A]): ValueOrTombstone[A] = {
    a.tombstone = true
    a
  }

  def aggregate(value: ValueOrTombstone[A]): ValueOrTombstone[A] =
    value
}
