package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.ivory.storage.legacy.PartitionFactThriftStorage
import com.ambiata.ivory.storage.metadata._
import com.ambiata.poacher.mr.ThriftSerialiser
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.saws.core._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration

import scalaz.{DList => _, _}, Scalaz._

object RepositoryBuilder {
  def repository: RIO[HdfsRepository] = for {
    d <- LocalTemporary.random.directory
    _ <- Directories.mkdirs(d)
    c <- IvoryConfigurationTemporary(d.path).conf
    r = HdfsRepository(HdfsLocation(d.path), c)
  } yield r

  def using[A](f: HdfsRepository => RIO[A]): RIO[A] =
    repository >>= (f(_))

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
    facts.foldLeftM(NonEmptyList(FactsetId.initial))({ case (factsetIds, facts) =>
      val groups = facts.groupBy(f => Partition(f.namespace, f.date)).toList
      groups.traverse({
        case (partition, facts) =>
          val out = repo.toIvoryLocation(Repository.factset(factsetIds.head) / partition.key / "data-output").location
          writeFacts(repo.configuration, facts, out)
      }).as(factsetIds.head.next.get <:: factsetIds)
    }).map(_.tail.reverse).flatMap(factsets =>
      RepositoryT.runWithRepo(repo, writeFactsetVersion(factsets)).map(_.last -> factsets))
  }

  def writeFacts(config: Configuration, facts: List[Fact], out: Location): RIO[Unit] =
    SequenceUtil.writeBytes(out, config, Clients.s3, None)(
      writer => RIO.safe(facts.foreach(f => writer(ThriftSerialiser().toBytes(f.toThrift)))))

  def uniqueFacts(allFacts: List[Fact]): List[Fact] =
    allFacts.filter(!_.isTombstone).groupBy(f => (f.entity, f.featureId)).toList.map(_._2.maxBy(_.date.int))

  def createSquash(repo: HdfsRepository, allFacts: List[Fact]): RIO[IvoryLocation] = {
    val serialiser = ThriftSerialiser()
    for {
      out <- Repository.tmpDir("squash").map(repo.toIvoryLocation)
      _   <- SequenceUtil.writeBytes(out.location, repo.configuration, Clients.s3, None)(
      writer => RIO.safe(uniqueFacts(allFacts).foreach(f => writer(serialiser.toBytes(f.toNamespacedThrift)))))
    } yield out
  }

  def factsFromIvoryFactset(repo: HdfsRepository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    PartitionFactThriftStorage.loadScoobiWith(repo, factset)

  def writeFactsetVersion(factsets: List[FactsetId]): RepositoryTIO[List[FeatureStoreId]] =
    factsets.traverseU(Factsets.updateFeatureStore).map(_.flatten)

}
