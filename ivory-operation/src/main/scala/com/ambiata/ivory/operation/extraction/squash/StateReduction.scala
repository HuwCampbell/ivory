package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.operation.extraction.reduction.Reduction

class StateReduction(windowStart: Date, windowEnd: Date, reduction: Reduction) extends Reduction {

  val tombstone = ThriftFactValue.t(new ThriftTombstone)
  val factPrev = createMutableFact
  // This is null by default
  factPrev.setFact(new ThriftFact)
  // This is just to ensure we don't "skip" an extra tombstone
  var first = true

  def clear(): Unit = {
    reduction.clear()
    // Reset the previous fact to be a tombstone, which comes in handy if we never see a fact before the window
    factPrev.setYyyyMMdd(windowStart.underlying)
    factPrev.getFact.setValue(tombstone)
    factPrev.getFact.setEntity("UNKNOWN")
    first = true
  }

  def update(fact: Fact): Unit = {
    // This is only needed for dump (for now)
    factPrev.getFact.setEntity(fact.entity)

    // If the current fact is within the window we update from the previous one,
    // which may be the first fact before the window or a tombstone
    if (Window.isFactWithinWindowRange(windowStart, windowEnd, fact)) {
      updateWithPreviousFact()
    } else if (!first) {
      reduction.skip(factPrev, "state-window")
    }
    first = false

    // Keep a reference to the current value/date for use in the next update or save
    // We're lucky (or unlucky) that thrift will recreate the value object each time and so it's safe (for now) to do this
    factPrev.getFact.setValue(fact.toThrift.getValue)
    factPrev.setYyyyMMdd(fact.toNamespacedThrift.getYyyyMMdd)
    ()
  }

  def skip(f: Fact, reason: String): Unit =
    reduction.skip(f, reason)

  def save: ThriftFactValue = {
    // _Always_ update the last fact, regardless of the window
    // This results in save not being idempotent, and if called too early will flush invalid facts
    updateWithPreviousFact()
    reduction.save
  }

  def updateWithPreviousFact(): Unit = {
    reduction.update(factPrev)
    ()
  }
}
