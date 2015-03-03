package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.poacher.mr._
import org.specs2._

class ChordFactsetMapperSpec extends Specification with ScalaCheck { def is = s2"""

  Counters are consistent across mapper  $counters
  Counter totals are correct             $totals

"""

  def counters = prop((context: ChordMapperSpecContext, priority: Priority) => {
    context.all.foreach(map(_, context, priority))
    (context.ok.counter + context.skip.counter) ==== context.all.size
  })

  def totals = prop((context: ChordMapperSpecContext, priority: Priority) => {
    context.all.foreach(map(_, context, priority))
    (context.ok.counter, context.skip.counter) ==== (
    (context.facts.size, context.skipped.size))
  })

  def map(f: Fact, context: ChordMapperSpecContext, priority: Priority): Unit = {
    ChordFactsetMapper.map(
        f.toNamespacedThrift
      , priority
      , Writables.bytesWritable(4096)
      , Writables.bytesWritable(4096)
      , context.emitter
      , context.ok
      , context.skip
      , context.serializer
      , FeatureIdIndex(0)
      , context.entities)
  }
}
