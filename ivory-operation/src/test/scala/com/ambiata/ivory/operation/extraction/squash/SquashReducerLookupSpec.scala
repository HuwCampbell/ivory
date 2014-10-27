package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ReducerLookup
import com.ambiata.poacher.mr.Writables
import org.specs2.{ScalaCheck, Specification}

class SquashReducerLookupSpec extends Specification with ScalaCheck { def is = s2"""

  Can calculate the number of reducers for a virtual feature      $lookup
  Can get the partition for a feature when no window              $partitionNoWindow
  Can get the partition for a feature based on the entity         $partitionEntity
"""

  def lookup = prop((d: VirtualDictionaryWindow, d2: Dictionary, s: Short, e: Int) => {
    val reducers = s & (Short.MaxValue - 1)
    val dict = (d.vd.dictionary append d2).byConcrete
    val (lookup, _) = SquashJob.dictToLookup(dict, Date.minValue)
    val create = SquashReducerLookup.create(dict, lookup, reducers)
    val lookupV = create.reducers.get(lookup.ids.get(d.vdict.vd.source.toString))
    // Just a santity test - we should do better though
    FeatureReducerOffset.getReducer(lookupV, e & Int.MaxValue) must beGreaterThanOrEqualTo(0)
  })

  def partitionNoWindow = prop((f: Fact, fids: Short, partitions: Short) => partitions != 0 ==> {
    val fid = Math.abs(fids).toShort
    val lookup = new ReducerLookup
    // To initialise the map
    lookup.putToReducers(-1, 0)
    val bw = Writables.bytesWritable(2048)
    SquashWritable.KeyState.set(f, bw, fid)
    SquashPartitioner.getPartition(lookup, bw, partitions) ==== fid % partitions
  })

  def partitionEntity = prop((f: Fact, fids: Short, e1: String, e2: String) => e1 != e2 ==> {
    val fid = Math.abs(fids).toShort
    val lookup = new ReducerLookup
    lookup.putToReducers(fid, FeatureReducerOffset(0, Short.MaxValue - 1).toInt)
    val bw = Writables.bytesWritable(2048)
    SquashWritable.KeyState.set(f.withEntity(e1), bw, fid)
    val a = SquashPartitioner.getPartition(lookup, bw, Integer.MAX_VALUE)
    SquashWritable.KeyState.set(f.withEntity(e2), bw, fid)
    val b = SquashPartitioner.getPartition(lookup, bw, Integer.MAX_VALUE)
    a !=== b
  })
}
