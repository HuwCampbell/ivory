package com.ambiata.ivory.benchmark

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.reduction._
import com.google.caliper._
import org.joda.time.{Days => JodaDays, LocalDate => JodaLocalDate, LocalDateTime => JodaLocalDateTime, Seconds => JodaSeconds}

/**
 * Benchmark showing the performance difference between
 * 1. Calculating the date offset every time
 * 2. Pre-calculating an array of spare representation to offset
 * 2. Pre-calculating an array of mostly-dense representation to offset
 *
 * At time of writing, option 2 is faster by a factor of 2 than calculating the date every time, and 1.5 times faster
 * than the compact array. However, summing up all the elements of the larger array is 150 times slower!!!
 *
 * For 2 years the size is ~ 512kb vs 2kb. For 50 years it's ~ 12mb vs 71kb.
 */
object DateOffsetBenchApp extends App {
  Runner.main(classOf[DateOffsetBench2Years], args)
//  Runner.main(classOf[DateOffsetBench50Years], args)
}

// More likely scenario of a year (plus a margin of a year)
class DateOffsetBench2Years extends DateOffsetBench(Date(2001, 6, 1), Date(2003, 6, 1)) {
  override def time_todays(n: Int) = super.time_todays(n)
  override def time_offsets_sparse(n: Int) = super.time_offsets_sparse(n)
  override def time_offsets_compact(n: Int) = super.time_offsets_compact(n)
  override def time_offsets_sparse_sum(n: Int) = super.time_offsets_sparse_sum(n)
  override def time_offsets_compact_sum(n: Int) = super.time_offsets_compact_sum(n)
}
// Worst case scenario - generates a 12MB array
class DateOffsetBench50Years extends DateOffsetBench(Date(2001, 1, 1), Date(2050, 12, 31)) {
  override def time_todays(n: Int) = super.time_todays(n)
  override def time_offsets_sparse(n: Int) = super.time_offsets_sparse(n)
  override def time_offsets_compact(n: Int) = super.time_offsets_compact(n)
  override def time_offsets_sparse_sum(n: Int) = super.time_offsets_sparse_sum(n)
  override def time_offsets_compact_sum(n: Int) = super.time_offsets_compact_sum(n)
}

class DateOffsetBench(start: Date, end:Date) extends SimpleScalaBenchmark {

  val dates = (start.int to end.int).flatMap(Date.fromInt).toList
  val dateOffsetsSparse = DateOffsets.sparse(start, end)
  val dateOffsetsCompact = DateOffsets.compact(start, end)

  println("Sparse array size: " + (dateOffsetsSparse.offsets.length * 4 / 1024) + "kb")
  println("Compact array size: " + (dateOffsetsCompact.offsets.length * 4 / 1024) + "kb")

  def time_todays(n: Int) =
    repeat(n) {
      var sum = 0
      dates.foreach {
        date => sum += DateTimeUtil.toDays(date)
      }
      sum
    }

  def time_offsets_sparse(n: Int) =
    repeat(n) {
      var sum = 0
      dates.foreach {
        date => sum += dateOffsetsSparse.get(date).value
      }
      sum
    }

  def time_offsets_compact(n: Int) =
    repeat(n) {
      var sum = 0
      dates.foreach {
        date => sum += dateOffsetsCompact.get(date).value
      }
      sum
    }

  // We also use the same array for summing the

  def time_offsets_sparse_sum(n: Int) =
    repeat(n) {
      var sum = 0
      dateOffsetsSparse.offsets.foreach {
        i => if (i != 1) sum += i
      }
      sum
    }

  def time_offsets_compact_sum(n: Int) =
    repeat(n) {
      var sum = 0
      dateOffsetsCompact.offsets.foreach {
        i => if (i != -1) sum += i
      }
      sum
    }
}
