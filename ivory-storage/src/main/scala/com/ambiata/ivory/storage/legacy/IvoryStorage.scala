package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress._
import scalaz.{DList => _, _}, Scalaz._


trait IvoryScoobiLoader[A] {
  def loadScoobi(implicit sc: ScoobiConfiguration): DList[ParseError \/ A]
}

trait IvoryScoobiStorer[A, +B] {
  def storeScoobi(dlist: DList[A])(implicit sc: ScoobiConfiguration): B
  def storeMeta: ScoobiAction[Unit] =
    ScoobiAction.ok(())
}

object IvoryStorage {
  // this is the version that factsets are written as
  val factsetVersion =
    FactsetVersionTwo

  def factsetStorer(repository: HdfsRepository, key: Key, codec: Option[CompressionCodec]): IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] =
    PartitionFactThriftStorageV2.PartitionedFactThriftStorer(repository, key, codec)

  /**
   * Get the loader for a given version
   */
  def factsetLoader(repo: HdfsRepository, version: FactsetVersion, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedFactThriftLoader(repo, factset, from, to).loadScoobi
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedFactThriftLoader(repo, factset, from, to).loadScoobi
  }

  def multiFactsetLoader(repo: HdfsRepository, version: FactsetVersion, factsets: List[Prioritized[FactsetId]], from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedMultiFactsetThriftLoader(repo, factsets, from, to).loadScoobi
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedMultiFactsetThriftLoader(repo, factsets, from, to).loadScoobi
  }

  def writeFactsetVersion(repo: Repository, factsets: List[FactsetId]): ResultTIO[Unit] =
    Versions.writeAll(repo, factsets, factsetVersion)

  def writeFactsetVersionI(factsets: List[FactsetId]): IvoryTIO[Unit] =
    IvoryT.fromResultT(writeFactsetVersion(_, factsets))

  implicit class IvoryFactStorage(dlist: DList[Fact]) {
    def toIvoryFactset(repo: HdfsRepository, factset: FactsetId, codec: Option[CompressionCodec])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] =
      IvoryStorage.factsetStorer(repo, Repository.factset(factset), codec).storeScoobi(dlist)
  }

  /** Facts */
  def factsFromIvoryStore(repo: HdfsRepository, store: FeatureStore): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, None, None)

  def factsFromIvoryStoreFrom(repo: HdfsRepository, store: FeatureStore, from: Date): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, Some(from), None)

  def factsFromIvoryStoreTo(repo: HdfsRepository, store: FeatureStore, to: Date): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, None, Some(to))

  def factsFromIvoryStoreBetween(repo: HdfsRepository, store: FeatureStore, from: Date, to: Date): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, Some(from), Some(to))

  /**
   * Create a DList of all the factsets in the given feature store optionally between two dates.
   * This will filter out any factsets which either have no version or no partitions
   */
  def factsFromIvoryStoreFor(repo: HdfsRepository, store: FeatureStore, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] = for {
    factsets <- ScoobiAction.fromHdfs(store.factsets.filterM(factset =>
                  Hdfs.exists(repo.toIvoryLocation(Repository.version(factset.value.id)).toHdfsPath).map(_ && !factset.value.partitions.isEmpty)))
    versions <- ScoobiAction.fromResultTIO(Versions.readPrioritized(repo, factsets.map(_.map(_.id))))
    byVersion: List[(FactsetVersion, List[Prioritized[FactsetId]])] = versions.groupBy(_._2).toList.map({ case (v, ids) => (v, ids.map(_._1)) })
    loaded   <- byVersion.traverseU({ case (v, ids) => IvoryStorage.multiFactsetLoader(repo, v, ids, from, to) })
  } yield if(loaded.isEmpty) DList[ParseError \/ (Priority, FactsetId, Fact)]() else loaded.reduce(_++_)

  def factsFromIvoryFactset(repo: HdfsRepository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, None, None)

  def factsFromIvoryFactsetFrom(repo: HdfsRepository, factset: FactsetId, from: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, Some(from), None)

  def factsFromIvoryFactsetTo(repo: HdfsRepository, factset: FactsetId, to: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, None, Some(to))

  def factsFromIvoryFactsetBetween(repo: HdfsRepository, factset: FactsetId, from: Date, to: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, Some(from), Some(to))

  def factsFromIvoryFactsetFor(repo: HdfsRepository, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = for {
    v  <- ScoobiAction.fromResultTIO(Versions.read(repo, factset))
    l  <- IvoryStorage.factsetLoader(repo, v, factset, from, to)
  } yield l

}
