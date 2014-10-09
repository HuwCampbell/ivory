package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._
import metadata._

import scalaz._, Scalaz._, \&/._, effect.IO
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._
import com.ambiata.mundane.parse.ListParser

import scalaz._, Scalaz._, \&/._, effect.IO

case class SnapshotMeta(snapshotId: SnapshotId, date: Date, featureStoreId: FeatureStoreId, commitId: Option[CommitId]) {

  lazy val stringLines: List[String] =
    List(date.string("-"), featureStoreId.render) ++ commitId.map(_.render)

  def order(other: SnapshotMeta): Ordering =
    (snapshotId, date, featureStoreId).?|?((other.snapshotId, other.date, other.featureStoreId))
}

object SnapshotMeta {

  val metaKeyName = KeyName.unsafe(".snapmeta")
  val allocated   = KeyName.unsafe(".allocated")

  implicit def SnapshotMetaOrder: Order[SnapshotMeta] =
    Order.order(_ order _)

  implicit def SnapshotMetaOrdering =
    SnapshotMetaOrder.toScalaOrdering

  def fromIdentifier(repository: Repository, id: SnapshotId): ResultTIO[Option[SnapshotMeta]] = {
    val path = Repository.snapshot(id) / metaKeyName
    for {
      exists      <- repository.store.exists(path)
      sm          <- if (exists) for {
      lines       <- repository.store.linesUtf8.read(path)
      // Ensure we have at least 3 lines (to include new commitId)
      safeLines    = if (lines.length == 2) lines ++ List("") else lines
      sm          <- ResultT.fromDisjunction[IO, SnapshotMeta](parser(id).run(safeLines).disjunction.leftMap(This.apply))
      } yield some(sm) else {
        // It's possible for snapshots to be allocated but either not deleted or still in progress
        println(s"WARNING: No $path found for ${id.render}")
        none.point[ResultTIO]
      }
    } yield sm
  }

  /**
   * Parse a snapshot meta file for a given snapshot id
   */
  def parser(snapshotId: SnapshotId): ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      date     <- localDate
      storeId  <- FeatureStoreId.listParser
      commitId <- string.map(CommitId.parse)
    } yield SnapshotMeta(snapshotId, Date.fromLocalDate(date), storeId, commitId)
  }

  /**
   * create a new Snapshot id by create a new .allocated sub-directory
   * with the latest available identifier + 1
   */
  def allocateId(repository: Repository): ResultTIO[SnapshotId] =
    IdentifierStorage.write(repository, Repository.snapshots, allocated, scodec.bits.ByteVector.empty).map(SnapshotId.apply)

  /**
   * create a new Snapshot meta object by allocating a new snapshot id
   */
  def createSnapshotMeta(repository: Repository, date: Date): ResultTIO[SnapshotMeta] = for {
    storeId     <- Metadata.latestFeatureStoreIdOrFail(repository)
    snapshotId  <- SnapshotMeta.allocateId(repository)
    commitId    <- Metadata.latestCommitId(repository)
  } yield SnapshotMeta(snapshotId, date, storeId, commitId)

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
  def latestUpToDateSnapshot(repository: Repository, date: Date): ResultTIO[Option[SnapshotMeta]] = {
    latestSnapshot(repository, date).flatMap(_.traverseU { meta: SnapshotMeta =>

      Metadata.latestFeatureStoreOrFail(repository).flatMap { featureStore =>
        if (meta.featureStoreId == featureStore.id) {

          if (meta.date == date) ResultT.ok[IO, Option[SnapshotMeta]](Some(meta))
          else
            FeatureStoreGlob.between(repository, featureStore, meta.date, date).map { glob =>
              if (glob.partitions.isEmpty) Some(meta)
              else                         None
            }
        } else ResultT.ok[IO, Option[SnapshotMeta]](None)
      }
    }).map(_.flatten)
  }

  def latestWithStoreId(repository: Repository, date: Date, featureStoreId: FeatureStoreId): ResultTIO[Option[SnapshotMeta]] =
    latestSnapshot(repository, date).map(_.filter(_.featureStoreId == featureStoreId))

  /**
   * get the latest snapshot which is just before a given date
   *
   * If there are 2 snapshots at the same date:
   *
   *   - take the snapshot having the greatest store id
   *   - if this results in 2 snapshots having the same store id, take the one having the greatest snapshot id
   *
   * This is implemented by defining an order on snapshots where we order based on the triple of
   *  (snapshotId, featureStoreId, date)
   *
   */
  def latestSnapshot(repository: Repository, date: Date): ResultTIO[Option[SnapshotMeta]] = for {
    ids      <- repository.store.listHeads(Repository.snapshots)
    metas    <- ids.traverseU(sid => SnapshotId.parse(sid.name).traverseU(id => fromIdentifier(repository, id)))
    filtered =  metas.flatten.flatten.filter(_.date isBeforeOrEqual date)
  } yield filtered.sorted.lastOption

  /** 
   * A snapshot is up to date if:
   *
   */

  /**
   * save the snapshot meta object to disk
   */
  def save(repository: Repository, snapshotMeta: SnapshotMeta): ResultTIO[Unit] =
    repository.store.linesUtf8.write(Repository.snapshot(snapshotMeta.snapshotId) / SnapshotMeta.metaKeyName, snapshotMeta.stringLines)

}