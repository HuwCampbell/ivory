package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Fact
import com.ambiata.ivory.core.thrift.ThriftFactValue

class CountReducer(var count: Long = 0) extends Reduction {

  val value = new ThriftFactValue()

  def clear(): Unit =
    count = 0

  def update(f: Fact): Unit =
    if (!f.isTombstone) count = count + 1

  def skip(f: Fact, reason: String): Unit = ()

  def save: ThriftFactValue = {
    value.setL(count)
    value
  }
}
