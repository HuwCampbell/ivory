package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._
//import metadata._

import scalaz._, Scalaz._, effect.IO
import scala.math.{Ordering => SOrdering}
import argonaut._, Argonaut._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._

sealed trait SnapshotMetadata
{
  private val legacyVersion : Long = 1

  def snapshotId: SnapshotId = this match {
    case SnapshotMetaLegacy(lm) => lm.snapshotId
    case SnapshotMetaJSON(jm)   => jm.snapshotId
  }

  def formatVersion: Long = this match {
    case SnapshotMetaLegacy(_)  => legacyVersion
    case SnapshotMetaJSON(jm)   => jm.formatVersion
  }

  def date: Date = this match {
    case SnapshotMetaLegacy(lm) => lm.date
    case SnapshotMetaJSON(jm)   => jm.date
  }

  def commitId: Option[CommitId] = this match {
    case SnapshotMetaLegacy(lm) => lm.commitId
    case SnapshotMetaJSON(jm)   => jm.commitId.pure[Option]
  }

  def featureIdOrCommitId: (FeatureStoreId \/ CommitId) = this match {
    case SnapshotMetaLegacy(lm) => (-\/)(lm.featureStoreId)
    case SnapshotMetaJSON(jm)   => (\/-)(jm.commitId)
  }

  def byKey[A](key: String)(implicit e: DecodeJson[A]): Option[A] = this match {
    case SnapshotMetaLegacy(_)  => none
    case SnapshotMetaJSON(jm)   => jm.others.as[A].toOption
  }

  def order(other: SnapshotMetadata): Ordering = (this, other) match {
    case (SnapshotMetaLegacy(_), SnapshotMetaJSON(_)) => Ordering.LT
    case (SnapshotMetaJSON(_), SnapshotMetaLegacy(_)) => Ordering.GT
    case (SnapshotMetaLegacy(lm1), SnapshotMetaLegacy(lm2)) => lm1 order lm2
    case (SnapshotMetaJSON(jm1), SnapshotMetaJSON(jm2)) => jm1 order jm2
  }
}

private case class SnapshotMetaLegacy(legacyMeta: legacy.SnapshotMeta) extends SnapshotMetadata
private case class SnapshotMetaJSON(jsonMeta: JSONSnapshotMeta) extends SnapshotMetadata

object SnapshotMetadata
{
  // data constructors

  private def snapshotMetaLegacy(legacyMeta: legacy.SnapshotMeta): SnapshotMetadata =
    new SnapshotMetaLegacy(legacyMeta)

  private def snapshotMetaJSON(jsonMeta: JSONSnapshotMeta): SnapshotMetadata =
    new SnapshotMetaJSON(jsonMeta)

  private val currentVersion : Long = 2
  private val allocated = KeyName.unsafe(".allocated")

  // exported functions:

  def createSnapshotMetadata(repo: Repository, date: Date): ResultTIO[SnapshotMetadata] = for {
    snapshotId <- allocateId(repo)
    dictionaryId <- Metadata.latestDictionaryIdFromIvory(repo)
    commitId <- Metadata.findOrCreateLatestCommitId(repo)
  } yield newSnapshotMeta(snapshotId, date, commitId)

  def fromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[SnapshotMetadata] = for {
    // try reading the JSON one first:
    jsonExists <- repo.store.exists(Repository.snapshot(id) / JSONSnapshotMeta.metaKeyName)

    x <- {
      if (jsonExists)
        jsonMetaFromIdentifier(repo, id).map(snapshotMetaJSON(_))
      else
        legacy.SnapshotMeta.fromIdentifier(repo, id).map(snapshotMetaLegacy(_))
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
  def latestUpToDateSnapshot(repo: Repository, date: Date): OptionT[ResultTIO, SnapshotMetadata] = for {
    meta <- latestSnapshot(repo, date)
    store <- Metadata.latestFeatureStoreOrFail(repo).liftM[OptionT]
    metaFeatureId <- getFeatureStoreId(repo, meta).liftM[OptionT]
    thereAreNoNewer <- checkForNewerFeatures(repo, metaFeatureId, store, meta.date, date).liftM[OptionT]

    x <- {
      type OptionResultTIO[A] = OptionT[ResultTIO, A]
      if (thereAreNoNewer)
        meta.pure[OptionResultTIO]
      else
        OptionT.optionT(none.pure[ResultTIO])
    }

  } yield x

  def latestWithStoreId(repo: Repository, date: Date, featureStoreId: FeatureStoreId): OptionT[ResultTIO, SnapshotMetadata] = for {
    meta <- latestSnapshot(repo, date)
    metaFeatureId <- getFeatureStoreId(repo, meta).liftM[OptionT]
    x <- {
      if (metaFeatureId == featureStoreId)
        OptionT.optionT(meta.some.pure[ResultTIO])
      else
        OptionT.optionT(none.pure[ResultTIO])
    }
  } yield x

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
  def latestSnapshot(repository: Repository, date: Date): OptionT[ResultTIO, SnapshotMetadata] = for {
    ids <- repository.store.listHeads(Repository.snapshots).liftM[OptionT]
    sids <- OptionT.optionT[ResultTIO](ids.traverseU((sid: Key) => SnapshotId.parse(sid.name)).pure[ResultTIO])
    metas <- sids.traverseU((sid: SnapshotId) => fromIdentifier(repository, sid)).liftM[OptionT]
    filtered = metas.filter(_.date isBeforeOrEqual date)
    meta <- OptionT.optionT[ResultTIO](filtered.sorted.lastOption.pure[ResultTIO])
  } yield meta

  def getFeatureStoreId(repo: Repository, meta: SnapshotMetadata): ResultTIO[FeatureStoreId] = meta.featureIdOrCommitId match {
    case -\/(fId) => fId.pure[ResultTIO]
    case \/-(cId) => Metadata.commitFromIvory(repo, cId).map(_.featureStoreId)
  }

  def featureStoreSnapshot(repo: Repository, meta: SnapshotMetadata): ResultTIO[legacy.FeatureStoreSnapshot] = for {
    storeId <- getFeatureStoreId(repo, meta)
    store <- Metadata.featureStoreFromIvory(repo, storeId)
  } yield legacy.FeatureStoreSnapshot(meta.snapshotId, meta.date, store)

  def save(repo: Repository, snapshotMeta: SnapshotMetadata): ResultTIO[Unit] = snapshotMeta match {
    // unfortunately since the legacy snapshot may or may not have a commit id, can't migrate it to
    // the json format.
    // since the existing snapshots are immutable however, i can't think of a reason why we would want to do this
    // anyway.
    case SnapshotMetaLegacy(lm) => legacy.SnapshotMeta.save(repo, lm)
    case SnapshotMetaJSON(jm)   => JSONSnapshotMeta.save(repo, jm)
  }

  // instances

  implicit def SnapshotMetadataOrder: Order[SnapshotMetadata] = Order.order(_ order _)

  implicit def SnapshotMetaOrdering: SOrdering[SnapshotMetadata] =
    SnapshotMetadataOrder.toScalaOrdering

  // helpers

  private def newSnapshotMeta(
    snapshotId: SnapshotId,
    date: Date,
    commitId: CommitId) = snapshotMetaJSON(JSONSnapshotMeta(snapshotId, currentVersion, date, commitId, baseJsonObject(snapshotId, currentVersion, date, commitId)))

  private def jsonMetaFromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[JSONSnapshotMeta] = for {
    jsonLines <- repo.store.linesUtf8.read(Repository.snapshot(id) / JSONSnapshotMeta.metaKeyName)
    // NOTE: (Dom) Better + neater way to do this?
    x <- fromJson(jsonLines.foldRight("")(_ + _)) match {
      case -\/(msg)       => ResultT.fail[IO, JSONSnapshotMeta]("failed to parse Snapshot metadata: " ++ msg)
      case \/-(jsonmeta)  => jsonmeta.pure[ResultTIO]
    }
  } yield x

  private def fromJson(json: String): (String \/ JSONSnapshotMeta) = Parse.decodeEither[JSONSnapshotMeta](json)

  private def checkForNewerFeatures(repo: Repository, metaFeatureId: FeatureStoreId, store: FeatureStore, beginDate: Date, endDate: Date): ResultTIO[Boolean] = {
    if (metaFeatureId == store.id) {
      if (beginDate == endDate)
        true.pure[ResultTIO]
      else
        FeatureStoreGlob.between(repo, store, beginDate, endDate).map(_.partitions.isEmpty)
    } else false.pure[ResultTIO]
  }

  /**
   * create a new Snapshot id by create a new .allocated sub-directory
   * with the latest available identifier + 1
   */
  private def allocateId(repo: Repository): ResultTIO[SnapshotId] =
    IdentifierStorage.write(repo, Repository.snapshots, allocated, scodec.bits.ByteVector.empty).map(SnapshotId.apply)

  private def baseJsonObject(snapshotId: SnapshotId, currentVersion: Long, date: Date, commitId: CommitId): Json=
    ("id" := snapshotId) ->: ("format_version" := currentVersion) ->: ("date" := date) ->: ("commit_id" := commitId) ->: jEmptyObject
}

private case class JSONSnapshotMeta(
  snapshotId: SnapshotId,
  formatVersion: Long,
  date: Date,
  commitId: CommitId,
  others: Json) {

  // version shouldn't be relevent to ordering.
  // NOTE (Dom): I'm guessing this is used to figure out the latest snapshot.
  // This seems "OK" right now since it doesnt seem to be used for much else.
  // But in time after more and more stuff gets added to it, will it still be appropriate
  // to be encoding this into the "semantics" of the metadata with Order[JSONSnapshotMeta]?
  //
  def order(other: JSONSnapshotMeta): Ordering =
    (snapshotId, date, commitId).?|?((other.snapshotId, other.date, other.commitId))

}

private object JSONSnapshotMeta {

  val metaKeyName = KeyName.unsafe(".metadata.json")

  implicit def JSONSnapshotMetaOrder: Order[JSONSnapshotMeta] =
    Order.order(_ order _)

  implicit def JSONSnapshotMetaOrdering: SOrdering[JSONSnapshotMeta] =
    JSONSnapshotMetaOrder.toScalaOrdering

  implicit def SnapshotMetaJSONCodec : CodecJson[JSONSnapshotMeta] = CodecJson(
    (_.others),
    ((c: HCursor) => for {
      id <- (c --\ "id").as[SnapshotId]
      version <- (c --\ "format_version").as[Long]
      date <- (c --\ "date").as[Date]
      commitId <- (c --\ "commit_id").as[CommitId]
      json <- c.as[Json]
    } yield JSONSnapshotMeta(id, version, date, commitId, json)))

  def save(repository: Repository, snapshotMeta: JSONSnapshotMeta): ResultTIO[Unit] =
    repository.store.linesUtf8.write(
      Repository.snapshot(snapshotMeta.snapshotId) / JSONSnapshotMeta.metaKeyName,
      // NOTE: (Dom) I'm assuming here that the list of strings is a list of lines to write to the file,
      // I've been burned by this assumption with the way i assumed the ListParser worked before,
      // I need to double check this case too.
      snapshotMeta.asJson.nospaces.pure[List])

}
