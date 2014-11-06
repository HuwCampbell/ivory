package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Date, DateTimeUtil}

case class DateOffsets(start: Int, end: Date, val count: Int, val offsets: Array[Int], f: DateOffsetLookup) {

  def get(d: Date): DateOffset =
    new DateOffset(offsets(f(d) - start))

  def untilEnd(d: Date): DateOffset =
    new DateOffset(get(end).value - get(d).value)

  /** Create a helper [[MutableDateSet]] based on the pre-calculated dates */
  def createSet: MutableDateSet = {
    val newOffsets = new Array[Int](offsets.length)
    System.arraycopy(offsets, 0, newOffsets, 0, newOffsets.length)
    val set = new MutableDateSet(start, newOffsets, f)
    set.clear()
    set
  }
}

/** A very specific data structure designed for set-like operations for dates */
class MutableDateSet(offset: Int, offsets: Array[Int], f: DateOffsetLookup) {

  def clear(): Unit = {
    var i = 0
    while (i < offsets.length) {
      if (offsets(i) != -1) {
        offsets(i) = 0
      }
      i += 1
    }
  }

  def inc(d: Date): Unit =
    offsets(f(d) - offset) += 1

  // WARNING: We require a function allocation for the callback

  def foreach(f: Int => Unit): Unit = {
    var i = 0
    while (i < offsets.length) {
      val v = offsets(i)
      if (v != -1) {
        f(v)
      }
      i += 1
    }
  }

  def fold[@specialized(Int, Long, Double) A](initial: A)(f: (A, Int) => A): A = {
    var v = initial
    foreach(i => v = f(v, i))
    v
  }

  /**
   * Start at the snapshot date and work backwards.
   * NOTE: We ignore anything that's not a full week (eg. the first few days a "month" period)
   * We could also create a separate MutableWeekSet
   */
  def foreachWeeks(f: Int => Unit): Unit =
    foreachBackwardsBuckets(7)(f)

  /** Optimize when the modularity is '1' */
  def foreachBuckets(bucketSize: Int)(f: Int => Unit): Unit =
    if (bucketSize == 1) foreach(f) else foreachBackwardsBuckets(bucketSize)(f)

  def foreachBackwardsBuckets(size: Int)(f: Int => Unit): Unit = {
    val rem = size - 1
    var i = offsets.length - 1
    // It's essential we have something that mods with size starting at 0, otherwise the "weeks" will be off
    var j = 0
    var count = 0
    while (i >= 0) {
      val v = offsets(i)
      if (v != -1) {
        count += v
        if (j % size == rem) {
          f(count)
          count = 0
        }
        j += 1
      }
      i -= 1
    }
  }

  def foldWeeks[@specialized(Int, Long, Double) A](initial: A)(f: (A, Int) => A): A = {
    var v = initial
    foreachWeeks(i => v = f(v, i))
    v
  }

  def countUnique: Int =
    fold(0)((count, i) => if (i != 0) count + 1 else count)

  def sortBuckets(a: Array[Int], b: Int): Unit = {
    var i = 0
    foreachBuckets(b) { j =>
      // Ignore whatever we can't fit into the last bucket - this might be part of a week
      if (i < a.length) a(i) = j
      i += 1
    }
    java.util.Arrays.sort(a)
  }
}

class DateOffset(val value: Int) extends AnyVal

class DateOffsetsLazy(private val run: () => DateOffsets) {
  lazy val dates: DateOffsets = run()
}

object DateOffsets {

  /** For sharing a single, lazy [[DateOffsets]] when calculating a number of expressions */
  def calculateLazyCompact(start: Date, end: Date): DateOffsetsLazy =
    new DateOffsetsLazy(() => compact(start, end))

  def compact(start: Date, end: Date): DateOffsets =
    calculate(start, end, DateOffsetsCompact)

  def sparse(start: Date, end: Date): DateOffsets =
    calculate(start, end, DateOffsetsSparse)

  def calculate(start: Date, end: Date, offset: DateOffsetLookup): DateOffsets = {
    // Calculate the number of days once
    val startDays = DateTimeUtil.toDays(start)
    val expected = DateTimeUtil.toDays(end) - startDays + 1
    val startOffset = offset(start)
    // Make sure we fill with -1's to represent dates that don't exist
    val offsets = Array.fill[Int](offset(end) - startOffset + 1)(-1)
    var i = 0
    var count = 0
    var d = start
    while(count < expected) {
      // Avoid boxing and unboxing an option with Date.fromInt()
      d = Date.unsafeFromInt(start.underlying + i)
      if (Date.isValid(d.year, d.month, d.day)) {
        offsets(offset(d) - startOffset) = startDays + count
        count += 1
      }
      i += 1
    }

    new DateOffsets(startOffset, end, count, offsets, offset)
  }
}

trait DateOffsetLookup {
  def apply(d: Date): Int
}

object DateOffsetsSparse extends DateOffsetLookup {

  final def apply(d: Date): Int =
    d.underlying
}

object DateOffsetsCompact extends DateOffsetLookup {

  /**
   * Because we're not dealing with 0-based values we need a number slightly larger than the expected 366.
   * 371 = 31 * 12 + 31 - 1 - 31
    */
  final def apply(d: Date): Int =
    d.day + (d.month * 31) + (d.year * 372)
}
