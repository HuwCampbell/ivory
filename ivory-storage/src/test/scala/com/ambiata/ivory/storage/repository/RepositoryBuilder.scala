package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryIvoryConfiguration._
import com.ambiata.ivory.core.thrift.NamespacedThriftFact
import com.ambiata.ivory.mr.FactFormats._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.poacher.mr.ThriftSerialiser
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.nicta.scoobi.Scoobi._

import scalaz._, Scalaz._, effect.IO

object RepositoryBuilder {

  def using[A](f: HdfsRepository => ResultTIO[A]): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    runWithConf(dir, conf => {
      val repo = HdfsRepository(HdfsLocation(dir.path), conf)
      f(repo)
    })
  }

  def createRepo(repo: HdfsRepository, dictionary: Dictionary, facts: List[List[Fact]]): ResultTIO[FeatureStoreId] = for {
    _      <- Metadata.dictionaryToIvory(repo, dictionary)
    stores <- createFacts(repo, facts)
  } yield stores._1

  def createFactset(repo: HdfsRepository, facts: List[Fact]): ResultTIO[FactsetId] =
    createFacts(repo, List(facts)).map(_._2.head)

  def createFacts(repo: HdfsRepository, facts: List[List[Fact]]): ResultTIO[(FeatureStoreId, List[FactsetId])] = {
    val serialiser = ThriftSerialiser()
    val factsets = facts.foldLeft(NonEmptyList(FactsetId.initial)) { case (factsetIds, facts) =>
      // This hack is because we can't pass a non-lazy Fact directly to fromLazySeq, but we want/need them to be props
      val bytes = facts.map(f => serialiser.toBytes(f.toNamespacedThrift))
      IvoryStorage.factsetStorer(repo, Repository.factset(factsetIds.head), None).storeScoobi(fromLazySeq(bytes).map {
        bytes => serialiser.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)
      })(repo.scoobiConfiguration).persist(repo.scoobiConfiguration)
      factsetIds.head.next.get <:: factsetIds
    }.tail.reverse
    RepositoryRead.fromRepository(repo).flatMap(r =>
      (for {
        _      <- IvoryStorage.writeFactsetVersionI(factsets)
        stores <- factsets.map(_ :: Nil).traverse(Metadata.incrementFeatureStore)
      } yield (stores.last, factsets)).run(r))
  }
}
