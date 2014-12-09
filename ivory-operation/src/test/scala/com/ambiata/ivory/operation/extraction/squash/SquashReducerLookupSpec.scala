package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ReducerLookup
import com.ambiata.poacher.mr.{ByteWriter, Writables}
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class SquashReducerLookupSpec extends Specification with ScalaCheck { def is = s2"""

  Can calculate the number of reducers for a virtual feature      $lookup
  Can get the partition for a feature when no window              partitionNoWindow
  Can get the partition for a feature based on the entity         partitionEntity
"""

  def lookup = {
    val reducers = 200
    val dict = Dictionary(List(
      Concrete(FeatureId(Namespace("pixel"), "pixel"), ConcreteDefinition(StringEncoding, Mode.Set, None, "", Nil)),
      Virtual(FeatureId(Namespace("a"), "v"), VirtualDefinition(FeatureId(Namespace("pixel"), "pixel"), Query.empty, Some(Window(12, Weeks)))),
      Concrete(FeatureId(Namespace("c"), "c"), ConcreteDefinition(StringEncoding, Mode.Set, None, "", Nil)),
      Virtual(FeatureId(Namespace("c"), "v"), VirtualDefinition(FeatureId(Namespace("c"), "c"), Query.empty, Some(Window(4, Weeks)))),
      Concrete(FeatureId(Namespace("b"), "b"), ConcreteDefinition(StringEncoding, Mode.Set, None, "", Nil)),
      Virtual(FeatureId(Namespace("b"), "v"), VirtualDefinition(FeatureId(Namespace("b"), "b"), Query.empty, Some(Window(2, Years))))
    ))
    val (lookup, _) = SquashJob.dictToLookup(dict.byConcrete, latest = true)
    val create = SquashReducerLookup.create(dict.byConcrete, lookup, reducers)
    def blah(lookup: Int): Unit= {
      val count = lookup & 0xffff
      println(count)
    }
    println(lookup)
    println(blah(create.reducers.get(0)))
    println(blah(create.reducers.get(1)))
    println(blah(create.reducers.get(2)))
    println(create)
    true
  }

  def lookup2 = prop((d: VirtualDictionaryWindow, d2: Dictionary, s: Short, e: Int) => {
    val reducers = s & (Short.MaxValue - 1)
    val dict = (d.vd.dictionary append d2).byConcrete
    val (lookup, _) = SquashJob.dictToLookup(dict, latest = true)
    val create = SquashReducerLookup.create(dict, lookup, reducers)
    val lookupV = create.reducers.get(lookup.ids.get(d.vdict.vd.source.toString))
    val windowReducers = create.reducers.values().asScala.map(_ & 0xffff).sum
    windowReducers must beGreaterThan(reducers - d2.byConcrete.sources.size) and
      (FeatureReducerOffset.getReducer(lookupV, e & Int.MaxValue) must beGreaterThanOrEqualTo(0))
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

  def partitionEntity = prop((f: Fact, fids: Short, e1: String, e2: String) => hashEntry(e1) != hashEntry(e2) ==> {
    val fid = Math.abs(fids).toShort
    val lookup = new ReducerLookup
    lookup.putToReducers(fid, FeatureReducerOffset(0, Short.MaxValue - 1).toInt)
    val bw = Writables.bytesWritable(4096)
    def getPartition(e: String): Int = {
      SquashWritable.KeyState.set(f.withEntity(e), bw, fid)
      SquashPartitioner.getPartition(lookup, bw, Integer.MAX_VALUE)
    }
    getPartition(e1) !=== getPartition(e2)
  })

  // Make sure we don't try to compare the partition for two entities with the same hash (different from String.hashCode)
  def hashEntry(s: String): Int = {
    val bw = Writables.bytesWritable(s.length * 4)
    val offset = ByteWriter.writeStringUTF8(bw.getBytes, s, 0)
    bw.setSize(offset)
    SquashWritable.GroupingByFeatureId.hashEntity(bw)
  }
}
