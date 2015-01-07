package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryIvoryConfiguration._
import com.ambiata.ivory.core.thrift.NamespacedThriftFact
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.ivory.storage.legacy.PartitionFactThriftStorageV2
import com.ambiata.ivory.storage.metadata._
import com.ambiata.poacher.mr.ThriftSerialiser
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.poacher.scoobi.ScoobiAction
import com.nicta.scoobi.Scoobi._

import scalaz.{DList => _, _}, Scalaz._

object RepositoryBuilder {

  def using[A](f: HdfsRepository => RIO[A]): RIO[A] = TemporaryDirPath.withDirPath { dir =>
    runWithConf(dir, conf => {
      val repo = HdfsRepository(HdfsLocation(dir.path), conf)
      f(repo)
    })
  }

  def createCommit(repo: HdfsRepository, dictionary: Dictionary, facts: List[List[Fact]]): RIO[Commit] = for {
    d          <- createDictionary(repo, dictionary)
    r          <- createFacts(repo, facts)
    (s, _)     =  r
    c          <- CommitStorage.findOrCreateLatestId(repo, d, s)
    store      <- FeatureStoreTextStorage.fromId(repo, s)
  } yield Commit(c, Identified(d, dictionary), store, None)

  def createDictionary(repo: HdfsRepository, dictionary: Dictionary): RIO[DictionaryId] =
    DictionaryThriftStorage(repo).store(dictionary)

  def createRepo(repo: HdfsRepository, dictionary: Dictionary, facts: List[List[Fact]]): RIO[FeatureStoreId] = for {
    _      <- createDictionary(repo, dictionary)
    stores <- createFacts(repo, facts)
  } yield stores._1

  def createFactset(repo: HdfsRepository, facts: List[Fact]): RIO[FactsetId] =
    createFacts(repo, List(facts)).map(_._2.head)

  def createFacts(repo: HdfsRepository, facts: List[List[Fact]]): RIO[(FeatureStoreId, List[FactsetId])] = {
    val serialiser = ThriftSerialiser()
    val factsets = facts.foldLeft(NonEmptyList(FactsetId.initial)) { case (factsetIds, facts) =>
      // This hack is because we can't pass a non-lazy Fact directly to fromLazySeq, but we want/need them to be props
      val bytes = facts.map(f => serialiser.toBytes(f.toNamespacedThrift))
      PartitionFactThriftStorageV2.PartitionedFactThriftStorer(repo, Repository.factset(factsetIds.head), None).storeScoobi(fromLazySeq(bytes).map {
        bytes => serialiser.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)
      })(repo.scoobiConfiguration).persist(repo.scoobiConfiguration)
      factsetIds.head.next.get <:: factsetIds
    }.tail.reverse
    RepositoryT.runWithRepo(repo, writeFactsetVersion(factsets)).map(_.last -> factsets)
  }
  def factsFromIvoryFactset(repo: HdfsRepository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    PartitionFactThriftStorageV2.loadScoobiWith(repo, factset)

  def writeFactsetVersion(factsets: List[FactsetId]): RepositoryTIO[List[FeatureStoreId]] =
    factsets.traverseU(Factsets.updateFeatureStore).map(_.flatten)

}
