package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.poacher.scoobi._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.io.compress._
import scalaz.{DList => _, _}


object IvoryStorage {
  // this is the version that factsets are written as
  val factsetVersion =
    FactsetVersionTwo

  def factsetStorer(repository: HdfsRepository, key: Key, codec: Option[CompressionCodec]): PartitionFactThriftStorageV2.PartitionedFactThriftStorer =
    PartitionFactThriftStorageV2.PartitionedFactThriftStorer(repository, key, codec)

  /**
   * Get the loader for a given version
   */
  def factsetLoader(repo: HdfsRepository, version: FactsetVersion, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = version match {
    case FactsetVersionOne => PartitionFactThriftStorageV1.PartitionedFactThriftLoader(repo, factset, from, to).loadScoobi
    case FactsetVersionTwo => PartitionFactThriftStorageV2.PartitionedFactThriftLoader(repo, factset, from, to).loadScoobi
  }


  def writeFactsetVersion(repo: Repository, factsets: List[FactsetId]): ResultTIO[Unit] =
    Versions.writeAll(repo, factsets, factsetVersion)

  def writeFactsetVersionI(factsets: List[FactsetId]): IvoryTIO[Unit] =
    IvoryT.fromResultT(writeFactsetVersion(_, factsets))

  def factsFromIvoryFactset(repo: HdfsRepository, factset: FactsetId): ScoobiAction[DList[ParseError \/ Fact]] =
    factsFromIvoryFactsetFor(repo, factset, None, None)

  def factsFromIvoryFactsetFor(repo: HdfsRepository, factset: FactsetId, from: Option[Date], to: Option[Date]): ScoobiAction[DList[ParseError \/ Fact]] = for {
    v  <- ScoobiAction.fromResultTIO(Versions.read(repo, factset))
    l  <- IvoryStorage.factsetLoader(repo, v, factset, from, to)
  } yield l

}
