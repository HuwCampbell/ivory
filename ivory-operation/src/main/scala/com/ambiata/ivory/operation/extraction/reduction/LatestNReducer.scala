package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Crash, Value, Fact}
import com.ambiata.ivory.core.thrift._
import scala.collection.JavaConverters._

case class ClockState[@specialized(Int, Float, Double, Boolean) A](val n: Int, var clock: Array[A], var hand: Int, var count: Int) {
  // Clicks the clock state around one increment
  def tick(value: A): Unit = {
      clock(hand) = value
      hand = (hand + 1) % n
      count = count + 1
  }
  // Unwinds the clock, returning the latest n entries (or fewer if there's not been many).
  def unwind: List[A] = {
    var i = Math.min(count, n)
    var xs: List[A] = Nil
    while (i > 0) {
      hand = if (hand == 0) n-1 else (hand - 1) % n
      i = i - 1
      xs = clock(hand) :: xs
    }
    xs.reverse
  }
}

class LatestNReducer(n: Int) extends Reduction {

  val a = ClockState[ThriftFactValue](n, new Array[ThriftFactValue](n), 0, 0)

  def clear(): Unit = {
    a.hand  = 0
    a.count = 0
  }

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
class LatestNStructReducer[@specialized(Int, Float, Double, Boolean) A : scala.reflect.ClassTag](seed: A, n: Int) extends ReductionFold[ClockState[A], A, List[A]] {

  val start = ClockState[A](n, Array.fill(n)(seed), 0, 0)

  def initial: ClockState[A] = start

  def fold(a: ClockState[A], value: A): ClockState[A] = {
    a.tick(value)
    a
  }

  def tombstone(a: ClockState[A]): ClockState[A] =
    a

  def aggregate(a: ClockState[A]): List[A] =
    a.unwind

}
