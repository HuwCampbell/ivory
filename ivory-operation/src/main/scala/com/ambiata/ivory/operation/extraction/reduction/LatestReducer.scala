package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.Fact
import com.ambiata.ivory.core.thrift.ThriftFactValue

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

  def save: ThriftFactValue =
    if (!tombstone) value else null
}
