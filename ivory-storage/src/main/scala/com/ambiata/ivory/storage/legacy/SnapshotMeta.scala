package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.storage.metadata.Metadata._

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

case class FeatureStoreSnapshot(snapshotId: SnapshotId, date: Date, store: FeatureStore)

object FeatureStoreSnapshot {
  def fromSnapshotId(repository: Repository, snapshotId: SnapshotId): ResultTIO[FeatureStoreSnapshot] =
    for {
      meta  <- SnapshotMeta.fromIdentifier(repository, snapshotId)
      store <- featureStoreFromIvory(repository, meta.featureStoreId)
    } yield FeatureStoreSnapshot(snapshotId, meta.date, store)
  
  def fromSnapshotIdAfter(repository: Repository, snapshotId: SnapshotId, date: Date): ResultTIO[Option[FeatureStoreSnapshot]] =
    fromSnapshotId(repository, snapshotId).map(snapshot => if (date isBefore snapshot.date) Some(snapshot) else None)
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

  def parser(snapshotId: SnapshotId): ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      date       <- localDate
      storeId    <- FeatureStoreId.listParser
    } yield SnapshotMeta(snapshotId, Date.fromLocalDate(date), storeId)
  }

  def allocateId(repo: Repository): ResultTIO[SnapshotId] = for {
    res <- IdentifierStorage.write(FilePath(".allocated"), scodec.bits.ByteVector.empty)(repo.toStore, Repository.snapshots)
  } yield { val (id, _) = res; SnapshotId(id) }

  def latest(repo: Repository, date: Date): ResultTIO[Option[SnapshotMeta]] = for {
    ids   <- repo.toReference(Repository.snapshots).run(s => p => StoreDataUtil.listDir(s, p)).map(_.map(_.basename.path))
    metas <- ids.traverseU(sid => SnapshotId.parse(sid).map(id => fromIdentifier(repo, id)).sequenceU)
    filtered = metas.collect { case Some(sm) if sm.date.isBeforeOrEqual(date) => sm }
  } yield filtered.sorted.lastOption
}
