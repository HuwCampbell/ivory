package com.ambiata.ivory.storage.legacy

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._
import metadata._

import scalaz._, Scalaz._, \&/._, effect.IO
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
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
   * save the snapshot meta object to disk
   */
  def save(repository: Repository, snapshotMeta: SnapshotMeta): ResultTIO[Unit] =
    repository.store.linesUtf8.write(Repository.snapshot(snapshotMeta.snapshotId) / SnapshotMeta.metaKeyName, snapshotMeta.stringLines)

}
