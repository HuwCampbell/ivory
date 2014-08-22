package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.{Prioritized, FeatureStore, Date, SnapshotId}
import com.ambiata.ivory.storage.fact.{FeatureStoreGlob, FactsetGlob}
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.repository.Repository
import com.ambiata.mundane.control._
import org.apache.commons.logging.LogFactory
import scalaz._, Scalaz._

/**
 * Identifier and date for the snapshot of a given store fully loaded in memory
 */
case class FeatureStoreSnapshot(snapshotId: SnapshotId, date: Date, store: FeatureStore)

object FeatureStoreSnapshot {
  private implicit val logger = LogFactory.getLog("ivory.storage.FeatureStoreSnapshot")

  def fromSnapshotMeta(repository: Repository): SnapshotMeta => ResultTIO[FeatureStoreSnapshot] = (meta: SnapshotMeta) =>
    featureStoreFromIvory(repository, meta.featureStoreId).map { store =>
      FeatureStoreSnapshot(meta.snapshotId, meta.date, store)
    }

  def fromSnapshotId(repository: Repository, snapshotId: SnapshotId): ResultTIO[FeatureStoreSnapshot] =
    SnapshotMeta.fromIdentifier(repository, snapshotId) >>= fromSnapshotMeta(repository)

  def fromSnapshotIdAfter(repository: Repository, snapshotId: SnapshotId, date: Date): ResultTIO[Option[FeatureStoreSnapshot]] =
    fromSnapshotId(repository, snapshotId).map(snapshot => Option(snapshot).filter(date isBefore _.date))

  /**
   * Given a previous Snapshot return the new factsets, up to a given date, as a list of globs (for the current store)
   */
  def newFactsetGlobs(repository: Repository, previousSnapshot: Option[SnapshotMeta], date: Date): ResultTIO[List[Prioritized[FactsetGlob]]] = for {
    currentFeatureStore  <- Metadata.latestFeatureStoreOrFail(repository)
    featureStoreSnapshot <- previousSnapshot.traverse(fromSnapshotMeta(repository))
    newFeatureStorePaths <- featureStoreSnapshot match {
      case None      => FeatureStoreGlob.before(repository, currentFeatureStore, date).map(_.globs)
      case Some(fss) =>
        for {
        // read facts from already processed store from the last snapshot date to the latest date
          oldOnes    <- FeatureStoreGlob.between(repository, fss.store, fss.date, date)
          difference =  currentFeatureStore diff fss.store
          _          =  logInfo(s"Reading factsets '${difference.factsets}' up to '$date'")
          // read factsets which haven't been seen up until the 'latest' date
          newOnes    <- FeatureStoreGlob.before(repository, difference, date)
        } yield oldOnes.globs ++ newOnes.globs
    }
  } yield newFeatureStorePaths

}
