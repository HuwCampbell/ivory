package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.thrift._
import com.ambiata.poacher.mr._
import org.apache.hadoop.io._
import org.specs2._

class ChordIncrementalMapperSpec extends Specification with ScalaCheck { def is = s2"""

  Counters are consistent across mapper  $counters
  Counter totals are correct             $totals

"""

  def counters = prop((context: ChordMapperSpecContext, priority: Priority) => {
    context.all.foreach(map(_, context, priority))
    (context.ok.counter + context.skip.counter + context.drop.counters.values.sum) ==== context.all.size
  })

  def totals = prop((context: ChordMapperSpecContext, priority: Priority) => {
    context.all.foreach(map(_, context, priority))
    context.ok.counter ==== context.facts.size and
     context.skip.counter ==== context.skipped.size and
     context.drop.counters.values.sum ==== context.dropped.size
  })

  def map(f: Fact, context: ChordMapperSpecContext, priority: Priority): Unit = {
    ChordIncrementalMapper.map(
      new NamespacedThriftFact with NamespacedThriftFactDerived
      , new BytesWritable(context.serializer.toBytes(f.toNamespacedThrift))
      , priority
      , Writables.bytesWritable(4096)
      , Writables.bytesWritable(4096)
      , context.emitter
      , context.ok
      , context.skip
      , context.drop
      , context.serializer
      , context.lookup
      , context.entities)
  }
}
