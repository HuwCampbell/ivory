package com.ambiata.ivory.storage.legacy

import scalaz._, Scalaz._, \&/._, effect.IO

import com.ambiata.mundane.io._
import com.ambiata.mundane.control._
import com.ambiata.mundane.parse.ListParser

import com.ambiata.ivory.core._
import com.ambiata.ivory.data._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._

case class SnapshotMeta(date: Date, store: FeatureStoreId) {

  def toReference(ref: ReferenceIO): ResultTIO[Unit] =
    ref.run(store => path => store.linesUtf8.write(path, stringLines))

  lazy val stringLines: List[String] =
    List(date.string("-"), store.render)

  def order(other: SnapshotMeta): Ordering =
    (date ?|? other.date) match {
      case Ordering.EQ => store ?|? other.store
      case o           => o
    }
}

object SnapshotMeta {

  val fname = FilePath(".snapmeta")

  implicit def SnapshotMetaOrder: Order[SnapshotMeta] =
    Order.order(_ order _)

  implicit def SnapshotMetaOrdering =
    SnapshotMetaOrder.toScalaOrdering

  def fromReference(ref: ReferenceIO): ResultTIO[SnapshotMeta] = for {
    lines <- ref.run(store => store.linesUtf8.read)
    sm    <- ResultT.fromDisjunction[IO, SnapshotMeta](parser.run(lines).disjunction.leftMap(This.apply))
  } yield sm

  def fromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[SnapshotMeta] =
    fromReference(repo.toReference(Repository.snapshots </> FilePath(id.render) </> fname))

  def parser: ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      d <- localDate
      s <- FeatureStoreId.listParser
    } yield SnapshotMeta(Date.fromLocalDate(d), s)
  }

  def allocateId(repo: Repository): ResultTIO[SnapshotId] = for {
    res <- IdentifierStorage.write(FilePath(".allocated"), scodec.bits.ByteVector.empty)(repo.toStore, Repository.snapshots)
  } yield { val (id, _) = res; SnapshotId(id) }

  def latest(repo: Repository, date: Date): ResultTIO[Option[(SnapshotId, SnapshotMeta)]] = for {
    ids   <- repo.toReference(Repository.snapshots).run(s => p => StoreDataUtil.listDir(s, p)).map(_.map(_.basename.path))
    metas <- ids.traverseU(sid => SnapshotId.parse(sid).map(id => fromIdentifier(repo, id).map((id, _))).sequenceU)
    filtered = metas.collect({ case Some((id, sm)) if sm.date.isBeforeOrEqual(date) => (id, sm) })
  } yield filtered.sortBy(_.swap).lastOption
}
