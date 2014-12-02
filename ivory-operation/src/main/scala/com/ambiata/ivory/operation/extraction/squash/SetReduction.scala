package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.core.{Date, Fact, Window}
import com.ambiata.ivory.operation.extraction.reduction.Reduction

/** A filter reduction that excludes facts outside of the window for set features only */
class SetReduction(windowStart: Date, windowEnd: Date, reduction: Reduction) extends Reduction {

  def clear(): Unit =
    reduction.clear()

  def update(f: Fact): Unit =
    if (Window.isFactWithinWindowRange(windowStart, windowEnd, f))
      reduction.update(f)
    else
      reduction.skip(f, "set-window")

  def skip(f: Fact, reason: String): Unit =
    reduction.skip(f, reason)

  def save: ThriftFactValue =
    reduction.save
}
