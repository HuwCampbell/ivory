package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.storage.fact.{FactsetGlob, FeatureStoreGlob}
import com.ambiata.ivory.storage.metadata._

import scalaz._, Scalaz._, \&/._, effect.IO
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._
import com.ambiata.mundane.parse.ListParser

import com.ambiata.ivory.core._
import com.ambiata.ivory.data._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._

case class SnapshotMeta(snapshotId: SnapshotId, date: Date, featureStoreId: FeatureStoreId) {

  def toReference(ref: ReferenceIO): ResultTIO[Unit] =
    ref.run(featureStore => path => featureStore.linesUtf8.write(path, stringLines))

  lazy val stringLines: List[String] =
    List(date.string("-"), featureStoreId.render)

  def order(other: SnapshotMeta): Ordering =
    (snapshotId, date, featureStoreId).?|?((other.snapshotId, other.date, other.featureStoreId))
}

object SnapshotMeta {

  val fname = FilePath(".snapmeta")

  implicit def SnapshotMetaOrder: Order[SnapshotMeta] =
    Order.order(_ order _)

  implicit def SnapshotMetaOrdering =
    SnapshotMetaOrder.toScalaOrdering

  def fromReference(ref: ReferenceIO): ResultTIO[SnapshotMeta] = for {
    lines      <- ref.run(store => store.linesUtf8.read)
    snapshotId <- ResultT.fromOption[IO, SnapshotId](SnapshotId.parse(ref.path.dirname.basename.path), s"can't parse ${ref.path.basename.path} as a snapshot id")
    sm         <- ResultT.fromDisjunction[IO, SnapshotMeta](parser(snapshotId).run(lines).disjunction.leftMap(This.apply))
  } yield sm

  def fromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[SnapshotMeta] =
    fromReference(repo.toReference(Repository.snapshots </> FilePath(id.render) </> fname))

  /**
   * Parse a snapshot meta file for a given snapshot id
   */
  def parser(snapshotId: SnapshotId): ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      date    <- localDate
      storeId <- FeatureStoreId.listParser
    } yield SnapshotMeta(snapshotId, Date.fromLocalDate(date), storeId)
  }

  /**
   * create a new Snapshot id by create a new .allocated sub-directory
   * with the latest available identifier + 1
   */
  def allocateId(repository: Repository): ResultTIO[SnapshotId] = for {
    res <- IdentifierStorage.write(FilePath(".allocated"), scodec.bits.ByteVector.empty)(repository.toStore, Repository.snapshots)
  } yield SnapshotId(res._1)

  /**
   * create a new Snapshot meta object by allocating a new snapshot id
   */
  def createSnapshotMeta(repository: Repository, date: Date): ResultTIO[SnapshotMeta] = for {
    storeId     <- Metadata.latestFeatureStoreIdOrFail(repository)
    snapshotId  <- SnapshotMeta.allocateId(repository)
  } yield SnapshotMeta(snapshotId, date, storeId)

  /**
   * get the latest snapshot which is just before a given date
   * and return it if it is up to date
   */
  def latestUpToDateSnapshot(repository: Repository, date: Date): ResultTIO[Option[SnapshotMeta]] =
    for {
      latest     <- latestSnapshot(repository, date)
      isUpToDate <- latest.traverse(isUpToDate(repository, date)).map(_.getOrElse(false))
    } yield if (isUpToDate) latest else None

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
    ids      <- repository.toReference(Repository.snapshots).run(s => p => StoreDataUtil.listDir(s, p)).map(_.map(_.basename.path))
    metas    <- ids.traverseU(sid => SnapshotId.parse(sid).map(id => fromIdentifier(repository, id)).sequenceU)
    filtered =  metas.flatten.filter(_.date isBeforeOrEqual date)
  } yield filtered.sorted.lastOption

  /** 
   * A snapshot is up to date if:
   * 
   *  - its date is after the required date
   *  - its date is before the required date but there is no new factsets just after the snapshot date and before the required date
   */
  def isUpToDate(repository: Repository, date: Date): SnapshotMeta => ResultTIO[Boolean] = (meta: SnapshotMeta) =>
    if (meta.date > date) ResultT.ok[IO, Boolean](true)
    else
      for {
        store      <- Metadata.latestFeatureStoreOrFail(repository)
        partitions <- FeatureStoreGlob.strictlyAfterAndBefore(repository, store, meta.date, date).map(_.partitions)
      } yield partitions.isEmpty

  /**
   * save the snapshot meta object to disk
   */
  def save(snapshotMeta: SnapshotMeta, output: ReferenceIO): ResultTIO[Unit] =
    snapshotMeta.toReference(output </> SnapshotMeta.fname)
  
}