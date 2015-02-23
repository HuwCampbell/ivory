package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Crash, Value, Fact}
import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._

case class ClockState[@specialized(Int, Long, Float, Double, Boolean) A](val clock: Array[A]) {
  var _count: Int =
    0

  def size: Int =
    clock.length

  def hand: Int =
    count % size

  def count: Int =
    _count

  // Clicks the clock state around one increment
  def tick(value: A): Unit = {
      clock(hand) = value
      _count = count + 1
  }

  // Unwinds the clock, returning the latest n entries (or fewer if there's not been many).
  def unwind: List[A] = {
    val unwound = clock.slice(0, hand).reverse ++ clock.slice(hand, math.min(size, count)).reverse
    unwound.toList
  }

  def reset(): Unit = {
    _count = 0
  }
}

class LatestNReducer(n: Int) extends Reduction {

  val a = ClockState[ThriftFactValue](new Array[ThriftFactValue](n))

  def clear(): Unit =
    a.reset()

  def update(fv: Fact): Unit = {
    val tv = fv.toThrift.getValue
    if (!tv.isSetT) {
      a.tick(tv)
    }
  }

  def skip(f: Fact, reason: String): Unit = ()

  def save: ThriftFactValue = {
    val xs = a.unwind
    ThriftFactValue.lst(new ThriftFactList(xs.map {
      case tv if tv.isSetStructSparse => ThriftFactListValue.s(tv.getStructSparse)
      case tv => Value.toPrimitive(tv) match {
        case Some(tpv) => ThriftFactListValue.p(tpv)
        case _         => Crash.error(Crash.CodeGeneration, s"You have hit an expression error as a list fact has been passed into the latestN reducer. This is a BUG.'")
      }
    }.asJava))
  }
}

/** Handle the latest of a single struct value */
class LatestNStructReducer[@specialized(Int, Long, Float, Double, Boolean) A : scala.reflect.ClassTag](seed: A, n: Int) extends ReductionFold[ClockState[A], A, List[A]] {

  val start = ClockState[A](Array.fill(n)(seed))

  def initial: ClockState[A] = {
    start.reset()
    start
  }

  def fold(a: ClockState[A], value: A): ClockState[A] = {
    a.tick(value)
    a
  }

  def tombstone(a: ClockState[A]): ClockState[A] =
    a

  def aggregate(a: ClockState[A]): List[A] =
    a.unwind

}
