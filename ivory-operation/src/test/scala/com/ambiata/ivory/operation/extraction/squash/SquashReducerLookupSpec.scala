package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.disorder.NaturalInt
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._
import com.ambiata.ivory.lookup.ReducerLookup
import com.ambiata.ivory.storage.arbitraries.DictionaryWithoutKeyedSet
import com.ambiata.poacher.mr.{ByteWriter, Writables}
import org.scalacheck.{Gen, Arbitrary}
import org.specs2.{ScalaCheck, Specification}
import scala.collection.JavaConverters._

class SquashReducerLookupSpec extends Specification with ScalaCheck { def is = s2"""

  Can calculate the number of reducers for a virtual feature      $lookup
  Can get the partition for a feature when no window              $partitionNoWindow
  Can get the partition for a feature based on the entity         $partitionEntity
  Proportion namespaces keeps all namespaces                      $proportionAllNamespaces
  The total of all partition reducers is >= total reducers        $proportionReducersTotal
  Lookup by window only keeps all features                        $lookupByWindowOnlyAllFeatures
  The total of all window reducers is >= total reducers           $lookupByWindowOnlyTotal
  Lookup by namespace size keeps all features                     $lookupByNamespaceSizeAndWindowAllFeatures
  The total of all sized reducers is >= total reducers            $lookupByNamespaceSizeAndWindowTotal
"""

  def lookup = prop((d: VirtualDictionaryWindow, d2: DictionaryWithoutKeyedSet, s: NaturalInt, e: Int) => s.value != Short.MaxValue ==> {
    val reducers = s.value.toShort
    val dict = d.vd.dictionary.append(d2.value)
    val create = SquashReducerLookup.toLookup(dict, SquashReducerLookup.calculateOffsets(SquashReducerLookup.lookupByWindowOnly(dict, reducers)))
    val lookupV = create.reducers.get(dict.byFeatureIndexReverse.getOrElse(d.vdict.vd.source, 0))
    val windowReducers = create.reducers.values().asScala.map(_ & 0xffff).sum
    windowReducers must beGreaterThan(reducers - d2.value.byConcrete.sources.size) and
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

  def proportionAllNamespaces = prop((ns: List[Sized[Namespace]], r: Int) =>
    SquashReducerLookup.proportionReducersPerNamespace(ns, r).keySet ==== ns.map(_.value).toSet
  )

  def proportionReducersTotal = prop((s: Sized[Namespace], ns: List[Sized[Namespace]], r: NaturalInt) => {
    val sns = (s :: ns).groupBy(_.value).mapValues(_.head).values.toList
    SquashReducerLookup.proportionReducersPerNamespace(sns, r.value).values.sum must beGreaterThanOrEqualTo(r.value)
  })

  def lookupByWindowOnlyAllFeatures = prop((d: Dictionary, r: NaturalInt) =>
    SquashReducerLookup.lookupByWindowOnly(d, r.value)
      .keySet ==== d.byConcrete.sources.keySet
  )

  def lookupByWindowOnlyTotal = prop((d: Dictionary, r: NaturalInt) =>
    SquashReducerLookup.lookupByWindowOnly(d, r.value)
      .values.sum must beGreaterThanOrEqualTo(r.value)
  )

  def lookupByNamespaceSizeAndWindowAllFeatures = prop((sd: SizedDictionary, r: NaturalInt) =>
    SquashReducerLookup.lookupByNamespaceSizeAndWindow(sd.dictionary, sd.sizes, r.value)
      .keySet ==== sd.dictionary.byConcrete.sources.keySet
  )

  def lookupByNamespaceSizeAndWindowTotal = prop((sd: SizedDictionary, r: NaturalInt) =>
    SquashReducerLookup.lookupByNamespaceSizeAndWindow(sd.dictionary, sd.sizes, r.value)
      .values.sum must beGreaterThanOrEqualTo(r.value)
  )

  case class SizedDictionary(l: List[(Sized[Namespace], List[ConcreteGroupFeature])]) {
    lazy val dictionary: Dictionary =
      l.flatMap(_._2.map(_.dictionary)).foldLeft(Dictionary.empty)(_.append(_))
    lazy val sizes: List[Sized[Namespace]] =
      l.map(_._1)
  }

  object SizedDictionary {

    implicit def SizedDictionaryArbitrary: Arbitrary[SizedDictionary] =
      Arbitrary(GenPlus.listOfSizedWithIndex(1, 10, genSizedNamespace).map(SizedDictionary.apply))

    def genSizedNamespace(i: Int): Gen[(Sized[Namespace], List[ConcreteGroupFeature])] = for {
      ns <- Arbitrary.arbitrary[Namespace].map(n => Namespace.unsafe(n.name + "_" + i))
      f  <- GenString.sensible
      cg <- GenPlus.listOfSizedWithIndex(1, 3, j => {
        val fid = FeatureId(ns, f + "_" + j)
        GenDictionary.concreteGroup(fid).map(ConcreteGroupFeature(fid, _))
      })
      sn <- GenRepository.sized(Gen.const(ns))
    } yield sn -> cg
  }
}
