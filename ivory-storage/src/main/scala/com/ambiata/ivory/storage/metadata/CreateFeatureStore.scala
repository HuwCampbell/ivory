package com.ambiata.ivory.storage.metadata

import scalaz._, Scalaz._, effect._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

object CreateFeatureStore {

  private implicit val logger = LogFactory.getLog("ivory.storage.metadata.CreateFeatureStore")

  def inRepository(repository: Repository, id: FeatureStoreId, sets: List[FactsetId], existing: Option[FeatureStoreId]): ResultTIO[Unit] = for {
    store    <- existing.traverse(e => Metadata.storeFromIvory(repository, e))
    tmp       = FeatureStoreTextStorage.fromFactsets(sets)
    newStore  = store.map(fs => tmp concat fs).getOrElse(tmp)
    _        <- Metadata.storeToIvory(repository, newStore, id)
  } yield ()

  // TODO handle locking
  def increment(repo: Repository, factset: FactsetId): ResultTIO[FeatureStoreId] = for {
    latest <- FeatureStoreTextStorage.latestId(repo)
    next   <- ResultT.fromOption[IO, FeatureStoreId](latest.map(_.next).getOrElse(Some(FeatureStoreId.initial)), "Run out of FeatureStore ids!")
    _       = logger.debug(s"Going to create feature store '${next}'" + latest.map(l => s" based off feature store '${l}'").getOrElse(""))
    _      <- inRepository(repo, next, List(factset), latest)
  } yield next
}
