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
    case SnapshotMetaJSON(jm) => jm.snapshotId
  }
}

case class SnapshotMetaLegacy(legacyMeta: legacy.SnapshotMeta) extends SnapshotMetadata
case class SnapshotMetaJSON(jsonMeta: JSONSnapshotMeta) extends SnapshotMetadata

object SnapshotMetadata
{
  def snapshotMetaLegacy(legacyMeta: legacy.SnapshotMeta): SnapshotMetadata =
    new SnapshotMetaLegacy(legacyMeta)

  def snapshotMetaJSON(jsonMeta: JSONSnapshotMeta): SnapshotMetadata =
    new SnapshotMetaJSON(jsonMeta)
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
