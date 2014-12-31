package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._
import com.ambiata.ivory.storage.manifest.SnapshotManifest
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

import scalaz._, Scalaz._

object SnapshotMetadataStorage {
  val allocated = KeyName.unsafe(".allocated")

  def byId(repository: Repository, id: SnapshotId): RIO[Option[SnapshotMetadata]] = for {
    manifest <- SnapshotManifest.io(repository, id).read
    metadata <- manifest.traverseU(toMetadata(repository, _))
  } yield metadata

  def byIdOrFail(repository: Repository, id: SnapshotId): RIO[SnapshotMetadata] =
    byId(repository, id).flatMap({
      case Some(s) =>
        s.pure[RIO]
      case None =>
        RIO.fail(s"Ivory invariant violated, could not locate snapshot metadata for $id")
    })


  def list(repository: Repository): RIO[List[SnapshotMetadata]] = for {
    ids <- repository.store.listHeads(Repository.snapshots).map(_.filterHidden)
    sids <- ids.flatMap(sid => SnapshotId.parse(sid.name).toList).pure[RIO]
    manifests <- sids.traverseU(SnapshotManifest.io(repository, _).read).map(_.flatten)
    metadatas <- manifests.traverseU(manifest => toMetadata(repository, manifest))
  } yield metadatas

  def featureStoreSnapshot(repo: Repository, meta: SnapshotMetadata): RIO[legacy.FeatureStoreSnapshot] =
    Metadata.featureStoreFromIvory(repo, meta.storeId)
      .map(store => legacy.FeatureStoreSnapshot(meta.id, meta.date, store))

  /**
   * get the latest snapshot which is just before a given date
   *
   * If there are 2 snapshots at the same date:
   *
   *   - take the snapshot having the greatest commit id
   *   - if this results in 2 snapshots having the same commit id, take the one having the greatest snapshot id
   *
   * This is implemented by defining an order on snapshots where we order based on the triple of
   *  (snapshotId, commitStoreId, date)
   *
   */
  def latestSnapshot(repository: Repository, date: Date): OptionT[RIO, SnapshotMetadata] = for {
    ids <- repository.store.listHeads(Repository.snapshots).liftM[OptionT]
    /* This looks fairly weird on first glance, but basically we should be ignoring snapshots that are invalid or
       incomplete. In fact this is a totally normal situation (parallel or aborted snapshots). */
    sids <- ids.flatMap(sid => SnapshotId.parse(sid.name).toList).pure[RIO].liftM[OptionT]
    metas <- sids.traverseU(SnapshotManifest.io(repository, _).read).map(_.flatten).liftM[OptionT]
    // FIX I think this should happen on SnapshotMetadata not the manifests.
    filtered = sort(metas.filter(_.date isBeforeOrEqual date))
    _ = println("Candidate snapshots: ")
    _ = filtered.map(m => s"  - id: ${m.snapshot}, date: ${m.date}.").foreach(println)
    meta <- OptionT.optionT[RIO](filtered.lastOption.pure[RIO])
    md <- toMetadata(repository, meta).liftM[OptionT]
    _ = println(s"Selected: ${meta.snapshot}")
  } yield md

  def sort(l: List[SnapshotManifest]): List[SnapshotManifest] =
    l.sortWith {
      case (sm1, sm2) => (sm1.storeOrCommit, sm2.storeOrCommit) match {
        case (-\/(s1), -\/(s2))   => (sm1.date, s1, sm1.snapshot).?|?((sm2.date, s2, sm2.snapshot)) == Ordering.LT
        case (-\/(_), \/-(_))    => true
        case (\/-(_), -\/(_))    => false
        case (\/-(c1), \/-(c2)) => (sm1.date, c1, sm1.snapshot).?|?((sm2.date, c2, sm2.snapshot)) == Ordering.LT
      }
    }

  def latestWithStoreId(repo: Repository, date: Date, featureStoreId: FeatureStoreId): OptionT[RIO, SnapshotMetadata] = for {
    meta <- latestSnapshot(repo, date)
    x <- {
      if (meta.storeId == featureStoreId)
        OptionT.some[RIO, SnapshotMetadata](meta)
      else
        OptionT.none[RIO, SnapshotMetadata]
    }
  } yield x

  /**
   * Get the latest snapshot which is just before a given date
   * and return it if it is up to date. The latest snapshot is up to date if
   *
   *  latestSnapshot.featureStore == latestFeatureStore
   *     and the snapshot.date == date
   *
   *     OR the snapshot.date <= date
   *        but there are no partitions between the snapshot date and date for factsets in the latest feature store
   */
  def latestUpToDateSnapshot(repo: Repository, date: Date): OptionT[RIO, SnapshotMetadata] = for {
    meta <- latestSnapshot(repo, date)
    store <- Metadata.latestFeatureStoreOrFail(repo).liftM[OptionT]
    thereAreNoNewer <- checkForNewerFeatures(repo, meta.storeId, store, meta.date, date).liftM[OptionT]

    x <- {
      type OptionRIO[A] = OptionT[RIO, A]
      if (thereAreNoNewer)
        meta.pure[OptionRIO]
      else
        OptionT.none[RIO, SnapshotMetadata]
    }

  } yield x

  // helpers
  private def checkForNewerFeatures(repo: Repository, metaFeatureId: FeatureStoreId, store: FeatureStore, beginDate: Date, endDate: Date): RIO[Boolean] = {
    if (metaFeatureId == store.id) {
      if (beginDate == endDate)
        true.pure[RIO]
      else
        FeatureStoreGlob.strictlyAfterAndBefore(repo, store, beginDate, endDate).map(_.partitions.isEmpty)
    } else false.pure[RIO]
  }

  def createSnapshotManifest(repo: Repository, date: Date): RIO[SnapshotManifest] = for {
    snapshotId <- allocateId(repo)
    commitId <- Metadata.findOrCreateLatestCommitId(repo)
  } yield SnapshotManifest.createLatest(commitId, snapshotId, date)

  def toMetadata(repo: Repository, meta: SnapshotManifest): RIO[SnapshotMetadata] =
    meta.storeOrCommit.fold(
      s => (s, none).pure[RIO],
      commitId => Metadata.commitFromIvory(repo, commitId).map(c => c.featureStoreId -> c.dictionaryId.some)
    ).map(x => SnapshotMetadata(meta.snapshot, meta.date, x._1, x._2))

  /**
   * create a new Snapshot id by create a new .allocated sub-directory
   * with the latest available identifier + 1
   */
  private def allocateId(repo: Repository): RIO[SnapshotId] =
    IdentifierStorage.write(repo, Repository.snapshots, allocated, scodec.bits.ByteVector.empty).map(SnapshotId.apply)
}
