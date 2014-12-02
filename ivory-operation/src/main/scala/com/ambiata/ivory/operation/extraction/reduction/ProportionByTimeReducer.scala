package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.{Time, Fact}
import com.ambiata.ivory.core.thrift.ThriftFactValue

// WARNING: This is just here for reference, we _need_ the value to be a local date
class ProportionByTimeReducer(start: Time, end: Time) extends Reduction {

  val value = new ThriftFactValue
  var count: Int = 0
  var total: Int = 0

  def clear(): Unit = {
    value.clear()
    count = 0
    total = 0
  }

  def update(f: Fact): Unit = {
    if (!f.isTombstone) {
      val time = f.time
      if (start.underlying <= time.underlying && time.underlying <= end.underlying) {
        count += 1
      }
    }
    // NOTE: Total includes tombstone entries
    total += 1
  }

  def skip(f: Fact, reason: String): Unit = ()

  def save: ThriftFactValue = {
    value.setD(if (total == 0) 0 else count / total.toDouble)
    value
  }
}
