package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.{NamespacedThriftFact, ThriftSerialiser}
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.Temporary
import com.nicta.scoobi.Scoobi._

import scalaz._

object RepositoryBuilder {

  def using[A](f: HdfsRepository => ResultTIO[A]): ResultTIO[A] = Temporary.using { dir =>
    val sc = ScoobiConfiguration()
    sc.set("hadoop.tmp.dir", dir.path)
    val repo = HdfsRepository(dir, RepositoryConfiguration(sc.configuration))
    f(repo)
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
      IvoryStorage.IvoryFactStorage(fromLazySeq(bytes).map {
        bytes => serialiser.fromBytesUnsafe(new NamespacedThriftFact with NamespacedThriftFactDerived, bytes)
      }).toIvoryFactset(repo, factsetIds.head, None)(repo.scoobiConfiguration).persist(repo.scoobiConfiguration)
      factsetIds.head.next.get <:: factsetIds
    }.tail.reverse
    (for {
      _      <- IvoryStorage.writeFactsetVersionI(factsets)
      stores <- Metadata.incrementFeatureStore(factsets)
    } yield (stores, factsets)).run(IvoryRead.testing(repo))
  }
}
