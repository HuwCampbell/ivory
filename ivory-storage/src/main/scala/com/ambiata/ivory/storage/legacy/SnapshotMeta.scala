package com.ambiata.ivory.storage.legacy

import scalaz._, Scalaz._, \&/._, effect._

import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.parse.ListParser

import com.ambiata.ivory.core._
import com.ambiata.ivory.data._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.storage.repository._

case class SnapshotMeta(date: Date, store: String) {

  def toReference(ref: ReferenceIO): ResultTIO[Unit] =
    ref.run(store => path => store.linesUtf8.write(path, stringLines))

  lazy val stringLines: List[String] =
    List(date.string("-"), store)

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

  def fromIdentifier(repo: Repository, id: Identifier): ResultTIO[SnapshotMeta] =
    fromReference(repo.toReference(Repository.snapshots </> FilePath(id.render) </> fname))

  def parser: ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      d <- localDate
      s <- string.nonempty
    } yield SnapshotMeta(Date.fromLocalDate(d), s)
  }

  def latest(snapshots: ReferenceIO, date: Date): ResultTIO[Option[(Identifier, SnapshotMeta)]] = for {
    paths <- snapshots.run(s => p => StoreDataUtil.listDir(s, p)).map(_.map(snapshots </> _.basename))
    metas <- paths.traverseU(p => {
      val snapmeta = p </> fname
      snapmeta.run(store => path => store.exists(path).flatMap(e =>
        if(e) fromReference(snapmeta).map(sm => Identifier.parse(p.path.basename.path).map((_, sm))) else ResultT.ok(None)))
    }).map(_.flatten)
    filtered = metas.filter(_._2.date.isBeforeOrEqual(date))
  } yield filtered.sortBy({ case (id, sm) => (sm, id) }).lastOption
}
