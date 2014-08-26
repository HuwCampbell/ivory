package com.ambiata.ivory.operation.rename

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.core.thrift.{NamespacedThriftFact, ThriftSerialiser}
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.TestConfigurations._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy.{IvoryStorage, SampleFacts}
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.repository.{RepositoryConfiguration, HdfsRepository, ScoobiRun}
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io.Temporary
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.nicta.scoobi.Scoobi._
import org.joda.time.LocalDate
import org.specs2.{ScalaCheck, Specification}
import scalaz.scalacheck.ScalazArbitrary._

import scalaz.{Name => _, _}, Scalaz._

class RenameSpec extends Specification with ScalaCheck with SampleFacts{ def is = s2"""
Rename
======
  validate ok                                        $validateOk
  validate missing a feature fails                   $validateFail

  rename a dictionary with a mapping                 $renameDictionary

  rename of a multiple features                      $renameAll
  rename of a one feature                            $renameOneFeature
  rename of a factset respect priority and time      $renamePriority
"""

  def validateOk = prop((dict: Dictionary, meta: ConcreteDefinition, fid1: FeatureId, fid2: FeatureId) => !dict.byFeatureId.contains(fid1) ==> {
    Rename.validate(RenameMapping(List(fid1 -> fid2)), dict.append(Dictionary(List(meta.toDefinition(fid1))))).toEither must beRight
  })

  def validateFail = prop((dict: Dictionary, fid1: FeatureId, fid2: FeatureId) => !dict.byFeatureId.contains(fid1) ==> {
    Rename.validate(RenameMapping(List(fid1 -> fid2)), dict).toEither must beLeft
  })

  def renameDictionary = prop((dict: Dictionary, featureId: FeatureId, meta: ConcreteDefinition, newFeature: FeatureId) =>
    !(dict.byFeatureId.contains(featureId) || dict.byFeatureId.contains(newFeature)) ==> {
      Rename.renameDictionary(RenameMapping(List(featureId -> newFeature)),
        dict.append(Dictionary(List(meta.toDefinition(featureId))))) ==== Dictionary(List(meta.toDefinition(newFeature)))
    })

  def renameAll = prop((mappingNel: NonEmptyList[(FeatureId, FeatureId, Fact)], meta: ConcreteDefinition) => {
    val mapping = mappingNel.toList
    val dictionary = Dictionary(mapping.map(_._1).map(fid => meta.toDefinition(fid)))
    renameWithFacts(RenameMapping(mapping.map(f => f._1 -> f._2)), dictionary,
      List(mapping.map { case (fid, _, fact) => fact.withFeatureId(fid)})
    ).map(r => r._1 -> r._2.toSet) must beOkValue (
      RenameStats(mapping.length) -> mapping.map { case (_, fid, fact) => fact.withFeatureId(fid)}.toSet
    )
  }) set(minTestsOk = 1, minSize = 1, maxSize = 5)

  def renameOneFeature = prop((mapping: (FeatureId, FeatureId), facts: NonEmptyList[Fact], meta: ConcreteDefinition) => {
    val dictionary = Dictionary(List(meta.toDefinition(mapping._1)))
    renameWithFacts(RenameMapping(List(mapping)), dictionary,
      List(facts.toList.map(_.withFeatureId(mapping._1)))
    ).map(r => r._1 -> r._2.toSet) must beOkValue (
      RenameStats(facts.length) -> facts.map(_.withFeatureId(mapping._2)).toSet
    )
  }) set(minTestsOk = 1, minSize = 1, maxSize = 5)

  def renamePriority = {
    val id = FeatureId("ns1", "fid1")
    val other = FeatureId("ns1", "fid2")
    val tid = FeatureId("ns2", "fid3")
    def f(d: Int, t: Int, v: String, fid: FeatureId): Fact =
      StringFact("eid1", fid, Date.fromLocalDate(new LocalDate(2012, 9, d)), Time.unsafe(t), v)
    val mapping = RenameMapping(List(id -> tid))
    val dictionary = Dictionary(List(
      Definition.concrete(id, StringEncoding, None, "", Nil),
      Definition.concrete(tid, StringEncoding, None, "", Nil)
    ))
    // This tests that identical entities handle priorities correct
    renameWithFacts(mapping, dictionary, List(
      Seq(f(1, 0, "1a", id), f(1, 3, "1b", other), f(2, 2, "1d", id)),
      Seq(f(1, 0, "2a", id), f(2, 1, "2d", id))
    )).map(r => r._1 -> r._2.toSet) must beOkValue(
      RenameStats(3) -> Set(f(2, 1, "2d", tid), f(2, 2, "1d", tid), f(1, 0, "2a", tid))
    )
  }

  def renameWithFacts(mapping: RenameMapping, dictionary: Dictionary, input: List[Seq[Fact]]): ResultTIO[(RenameStats, Seq[Fact])] = Temporary.using { dir =>
    implicit val sc: ScoobiConfiguration = scoobiConfiguration

    val repo = HdfsRepository(dir, RepositoryConfiguration(sc.configuration))

    val serialiser = ThriftSerialiser()
    val factsets = input.foldLeft(NonEmptyList(FactsetId.initial)) { case (factsetIds, facts) =>
      // This hack is because we can't pass a non-lazy Fact directly to fromLazySeq, but we want/need them to be props
      val bytes = facts.map(f => serialiser.toBytes(f.toNamespacedThrift))
      IvoryStorage.IvoryFactStorage(fromLazySeq(bytes).map {
        bytes => serialiser.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)
      }).toIvoryFactset(repo, factsetIds.head, None)(sc).persist
      factsetIds.head.next.get <:: factsetIds
    }.tail.reverse
    (for {
      _      <- IvoryStorage.writeFactsetVersionI(factsets)
      _      <- factsets.traverse(Metadata.incrementFeatureStore)
      _      <-  Metadata.dictionaryToIvoryT(dictionary)
      result <- Rename.rename(mapping, 10.mb)
      facts  <- IvoryT.fromResultT(IvoryStorage.factsFromIvoryFactset(_, result._1).run(sc).map(_.run))
    } yield (result._3, facts)).run(IvoryRead.testing(repo)).map {
      case (s, facts) => (s, facts.flatMap(_.toOption))
    }
  }
}
