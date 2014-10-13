package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._
//import metadata._

//import scalaz._, Scalaz._, \&/._, effect.IO
import scalaz._, Scalaz._, effect.IO
import argonaut._, Argonaut._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._

sealed trait SnapshotMetadata
{
  def snapshotId(x: SnapshotMetadata): SnapshotId = this match {
    case SnapshotMetaLegacy(lm) => {
      lm.snapshotId
    }
    case SnapshotMetaJSON(jm)   => jm.snapshotId
  }

  def formatVersion(x: SnapshotMetadata): Long = this match {
    case SnapshotMetaLegacy(_)  => 1
    case SnapshotMetaJSON(jm)   => jm.formatVersion
  }

  def date(x: SnapshotMetadata): Date = this match {
    case SnapshotMetaLegacy(lm) => lm.date
    case SnapshotMetaJSON(jm)   => jm.date
  }

  def commitId(x: SnapshotMetadata): Option[CommitId] = this match {
    case SnapshotMetaLegacy(lm) => lm.commitId
    case SnapshotMetaJSON(jm)   => jm.commitId.pure[Option]
  }
}

case class SnapshotMetaLegacy(legacyMeta: legacy.SnapshotMeta) extends SnapshotMetadata
case class SnapshotMetaJSON(jsonMeta: JSONSnapshotMeta) extends SnapshotMetadata

object SnapshotMetadata
{
  // data constructors

  def snapshotMetaLegacy(legacyMeta: legacy.SnapshotMeta): SnapshotMetadata =
    new SnapshotMetaLegacy(legacyMeta)

  def snapshotMetaJSON(jsonMeta: JSONSnapshotMeta): SnapshotMetadata =
    new SnapshotMetaJSON(jsonMeta)

  def fromIdentifier(repo: Repository, id: SnapshotId): OptionT[ResultTIO, legacy.SnapshotMeta] = for {
    // try reading the JSON one first:
    jsonLines <- repository.store.linesUtf8.read(Repository.snapshot(id) / JSONSnapshotMeta.metaKeyName).liftM[OptionT]
    // FIXME: Finish this
  } yield jsonLines

  /**
   * get the latest snapshot which is just before a given date
   *
   * If there are 2 snapshots at the same date:
   *
   *   - take the snapshot having the greatest store id
   *   - if this results in 2 snapshots having the same store id, take the one having the greatest snapshot id
   *
   * This is implemented by defining an order on snapshots where we order based on the triple of
   *  (snapshotId, commitStoreId, date)
   *
   */
  def latestSnapshot(repository: Repository, date: Date): OptionT[ResultTIO, SnapshotMetadata] = for {
    // ids :: [Key]
    ids <- repository.store.listHeads(Repository.snapshots).liftM[OptionT]
    // sids :: [SnapshotId]
    sids <- OptionT.optionT[ResultTIO](ids.traverseU((sid: Key) => SnapshotId.parse(sid.name)).pure[ResultTIO])
    // metas :: [SnapshotMetadata]
    // fromIdentifier repository :: SnapshotId -> ResultTIO SnapshotMetadata
    metas <- sids.traverseU((sid: SnapshotId) => fromIdentifier(repository, sid)).liftM[OptionT]
    filtered = metas.filter(_.date isBeforeOrEqual date)
    meta <- OptionT.optionT[ResultTIO](filtered.sorted.lastOption.pure[ResultTIO])
  } yield meta
}

// NOTE (Dom): formatting?
case class JSONSnapshotMeta(
    snapshotId: SnapshotId
  , formatVersion: Long
  , date: Date
  , commitId: CommitId) {

  // version shouldn't be relevent to ordering.
  // NOTE (Dom): I'm guessing this is used to figure out the latest snapshot.
  // This seems "OK" right now since it doesnt seem to be used for much else.
  // But in time after more and more stuff gets added to it, will it still be appropriate
  // to be encoding this into the "semantics" of the metadata with Order[JSONSnapshotMeta]?
  //
  def order(other: JSONSnapshotMeta): Ordering =
    (snapshotId, date, commitId).?|?((other.snapshotId, other.date, other.commitId))

}

object JSONSnapshotMeta {

  val metaKeyName = KeyName.unsafe(".metadata.json")

  implicit def SnapshotMetaJSONCodec : CodecJson[JSONSnapshotMeta] =
    casecodec4(JSONSnapshotMeta.apply, JSONSnapshotMeta.unapply)("id", "format_version", "date", "commit_id")

  def save(repository: Repository, snapshotMeta: JSONSnapshotMeta): ResultTIO[Unit] =
    repository.store.linesUtf8.write(
      Repository.snapshot(snapshotMeta.snapshotId) / JSONSnapshotMeta.metaKeyName,
      snapshotMeta.asJson.nospaces.pure[List])

}
