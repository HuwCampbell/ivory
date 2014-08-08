package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.storage.metadata.FeatureStoreTextStorage
import com.ambiata.saws.s3.S3

import scalaz.{DList => _, _}, Scalaz._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.repository._
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import com.ambiata.mundane.control._


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

  def factsetStorer(path: String, codec: Option[CompressionCodec]): IvoryScoobiStorer[Fact, DList[(PartitionKey, ThriftFact)]] =
    PartitionFactThriftStorageV2.PartitionedFactThriftStorer(path, codec)

  /**
   * Get the loader for a given version
   */
  def factsetLoader(repo: Repository, version: FactsetVersion, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedFactThriftLoader(repo, factset, from, to).loadScoobi
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedFactThriftLoader(repo, factset, from, to).loadScoobi
  }

  def multiFactsetLoader(repo: Repository, version: FactsetVersion, factsets: List[PrioritizedFactset], from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedMultiFactsetThriftLoader(repo, factsets, from, to).loadScoobi
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedMultiFactsetThriftLoader(repo, factsets, from, to).loadScoobi
  }

  def writeFactsetVersion(repo: Repository, factsets: List[FactsetId]): ResultTIO[Unit] =
    Versions.writeAll(repo, factsets, factsetVersion)

  implicit class IvoryFactStorage(dlist: DList[Fact]) {
    def toIvoryFactset(repo: HdfsRepository, factset: FactsetId, codec: Option[CompressionCodec])(implicit sc: ScoobiConfiguration): DList[(PartitionKey, ThriftFact)] =
      IvoryStorage.factsetStorer(repo.factset(factset).path, codec).storeScoobi(dlist)
  }

  /* Facts */
  def factsFromIvoryStore(repo: Repository, store: FeatureStore): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, None, None)

  def factsFromIvoryStoreFrom(repo: Repository, store: FeatureStore, from: Date): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, Some(from), None)

  def factsFromIvoryStoreTo(repo: Repository, store: FeatureStore, to: Date): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, None, Some(to))

  def factsFromIvoryStoreBetween(repo: Repository, store: FeatureStore, from: Date, to: Date): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] =
    factsFromIvoryStoreFor(repo, store, Some(from), Some(to))

  def factsFromIvoryStoreFor(repo: Repository, store: FeatureStore, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ (Priority, FactsetId, Fact)]] = for {
    factsets <- ScoobiAction.fromHdfs(store.factsets.traverse(factset => for {
                  ve <- Hdfs.exists(repo.version(factset.factsetId).toHdfs)
                  s  <- Hdfs.size(repo.factset(factset.factsetId).toHdfs)
                } yield (factset, ve && s.toBytes.value != 0))).map(_.collect({ case (factset, true) => factset }))
    versions <- ScoobiAction.fromResultTIO(factsets.traverseU(factset =>
      Versions.read(repo, factset.factsetId).map(factset -> _)))
    combined: List[(FactsetVersion, List[PrioritizedFactset])] = versions.groupBy(_._2).toList.map({ case (k, vs) => (k, vs.map(_._1)) })
    loaded   <- combined.traverseU({ case (v, fss) => IvoryStorage.multiFactsetLoader(repo, v, fss, from, to) })
  } yield if(loaded.isEmpty) DList[ParseError \/ (Priority, FactsetId, Fact)]() else loaded.reduce(_++_)

  def factsFromIvoryFactset(repo: Repository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, None, None)

  def factsFromIvoryFactsetFrom(repo: Repository, factset: FactsetId, from: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, Some(from), None)

  def factsFromIvoryFactsetTo(repo: Repository, factset: FactsetId, to: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, None, Some(to))

  def factsFromIvoryFactsetBetween(repo: Repository, factset: FactsetId, from: Date, to: Date): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, Some(from), Some(to))

  def factsFromIvoryFactsetFor(repo: Repository, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = for {
    v  <- ScoobiAction.fromResultTIO(Versions.read(repo, factset))
    l  <- IvoryStorage.factsetLoader(repo, v, factset, from, to)
  } yield l

}
