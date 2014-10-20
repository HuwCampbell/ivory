package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._

import scalaz._, Scalaz._, \&/._, effect.IO
import scala.math.{Ordering => SOrdering}
import argonaut._, Argonaut._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._

sealed trait SnapshotManifestVersion
{
  val long: Long
}

object SnapshotManifestVersionLegacy extends SnapshotManifestVersion
{
  val long: Long = 0
}

object SnapshotManifestVersionV1 extends SnapshotManifestVersion
{
  val long: Long = 1
}

object SnapshotManifestVersion
{
  implicit def SnapshotManifestVersionCodecJson: CodecJson[SnapshotManifestVersion] = CodecJson.derived(
    EncodeJson(_.long.asJson),
    DecodeJson.optionDecoder(_.as[Long].toOption.flatMap(fromLong), "SnapshotManifestVersion"))

  def fromLong(longVersion: Long): Option[SnapshotManifestVersion] = longVersion match {
    case SnapshotManifestVersionLegacy.long => SnapshotManifestVersionLegacy.some
    case SnapshotManifestVersionV1.long     => SnapshotManifestVersionV1.some
    case _                                  => none
  }
}

/**
 * Representation of Snapshot metadata stored on disk
 **/
case class SnapshotManifest(
  snapshotId: SnapshotId,
  formatVersion: SnapshotManifestVersion,
  date: Date,
  storeOrCommitId: (FeatureStoreId \&/ CommitId),
  others: Json) {

  def byKey[A: DecodeJson](key: String): Option[A] = others.field(key).flatMap(_.as[A].toOption)

  // version shouldn't be relevent to ordering.
  def order(other: SnapshotManifest): Ordering = (SnapshotManifest.eitherThat(storeOrCommitId), SnapshotManifest.eitherThat(other.storeOrCommitId)) match {
    case (-\/(_), \/-(_))   => Ordering.LT
    case (\/-(_), -\/(_))   => Ordering.GT
    case (-\/(f1), -\/(f2)) => (snapshotId, date, f1).?|?((other.snapshotId, other.date, f2))
    case (\/-(c1), \/-(c2)) => (snapshotId, date, c1).?|?((other.snapshotId, other.date, c2))
  }
}

object SnapshotManifest
{
  val metaKeyName = KeyName.unsafe(".metadata.json")

  private val allocated = KeyName.unsafe(".allocated")

  // exported functions:

  def fromSnapshotMetaLegacy(lm: legacy.SnapshotMeta): SnapshotManifest = {
    val cId: (FeatureStoreId \&/ CommitId) = lm.commitId.cata(Both(lm.featureStoreId, _), This(lm.featureStoreId))

    SnapshotManifest(
      lm.snapshotId,
      SnapshotManifestVersionLegacy,
      lm.date,
      cId,
      baseJsonObject(
        lm.snapshotId,
        SnapshotManifestVersionLegacy,
        lm.date,
        cId))
  }

  def snapshotManifest(
    snapshotId: SnapshotId,
    date: Date,
    storeOrCommitId: (FeatureStoreId \&/ CommitId),
    others: Json): SnapshotManifest = SnapshotManifest(
      snapshotId,
      SnapshotManifestVersionV1,
      date,
      storeOrCommitId,
      others)

  def createSnapshotManifest(repo: Repository, date: Date): ResultTIO[SnapshotManifest] = for {
    snapshotId <- allocateId(repo)
    storeOrCommitId <- Metadata.findOrCreateLatestCommitId(repo)
  } yield newSnapshotMeta(snapshotId, date, storeOrCommitId)

  def fromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[SnapshotManifest] = for {
    // try reading the JSON one first:
    jsonExists <- repo.store.exists(Repository.snapshot(id) / SnapshotManifest.metaKeyName)

    x <- {
      if (jsonExists)
        jsonMetaFromIdentifier(repo, id)
      else
        legacy.SnapshotMeta.fromIdentifier(repo, id).map(fromSnapshotMetaLegacy(_))
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
  def latestUpToDateSnapshot(repo: Repository, date: Date): OptionT[ResultTIO, SnapshotManifest] = for {
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

  def latestWithStoreId(repo: Repository, date: Date, featureStoreId: FeatureStoreId): OptionT[ResultTIO, SnapshotManifest] = for {
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
  def latestSnapshot(repository: Repository, date: Date): OptionT[ResultTIO, SnapshotManifest] = for {
    ids <- repository.store.listHeads(Repository.snapshots).liftM[OptionT]
    sids <- OptionT.optionT[ResultTIO](ids.traverseU((sid: Key) => SnapshotId.parse(sid.name)).pure[ResultTIO])
    metas <- sids.traverseU((sid: SnapshotId) => fromIdentifier(repository, sid)).liftM[OptionT]
    filtered = metas.filter(_.date isBeforeOrEqual date)
    meta <- OptionT.optionT[ResultTIO](filtered.sorted.lastOption.pure[ResultTIO])
  } yield meta

  def getFeatureStoreId(repo: Repository, meta: SnapshotManifest): ResultTIO[FeatureStoreId] = eitherThis(meta.storeOrCommitId).fold(
    _.pure[ResultTIO],
    Metadata.commitFromIvory(repo, _).map(_.featureStoreId))

  def featureStoreSnapshot(repo: Repository, meta: SnapshotManifest): ResultTIO[legacy.FeatureStoreSnapshot] = for {
    storeId <- getFeatureStoreId(repo, meta)
    store <- Metadata.featureStoreFromIvory(repo, storeId)
  } yield legacy.FeatureStoreSnapshot(meta.snapshotId, meta.date, store)

  def save(repository: Repository, snapshotMeta: SnapshotManifest, cId: CommitId): ResultTIO[Unit] = {
    // option is set to none if the metadata already has a Commit ID
    val commit: Option[CommitId] = snapshotMeta.storeOrCommitId.b.cata(_ => none, cId.pure[Option])
    val json = commit.map("commit_id":= _) ->?: snapshotMeta.asJson

    repository.store.utf8.write(
      Repository.snapshot(snapshotMeta.snapshotId) / SnapshotManifest.metaKeyName,
      json.nospaces)
  }

  // instances

  implicit def SnapshotManifestOrder: Order[SnapshotManifest] = Order.order(_ order _)

  implicit def SnapshotManifestSOrdering: SOrdering[SnapshotManifest] =
    SnapshotManifestOrder.toScalaOrdering

  implicit def SnapshotManifestCodecJson : CodecJson[SnapshotManifest] = CodecJson(
    (_.others),
    ((c: HCursor) => for {
      id <- (c --\ "id").as[SnapshotId]
      version <- (c --\ "format_version").as[SnapshotManifestVersion]
      date <- (c --\ "date").as[Date]
      commitId <- (c --\ "commit_id").as[CommitId]
      json <- c.as[Json]
    } yield SnapshotManifest(id, version, date, That(commitId), json)))

  // helpers

  private def newSnapshotMeta(
    snapshotId: SnapshotId,
    date: Date,
    commitId: CommitId) = SnapshotManifest(snapshotId, SnapshotManifestVersionV1, date, wrapThat(commitId), baseJsonObject(snapshotId, SnapshotManifestVersionV1, date, That(commitId)))

  private def jsonMetaFromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[SnapshotManifest] = for {
    json <- repo.store.utf8.read(Repository.snapshot(id) / SnapshotManifest.metaKeyName)
    x <- fromJson(json) match {
      case -\/(msg)       => ResultT.fail[IO, SnapshotManifest]("failed to parse Snapshot manifest: " ++ msg)
      case \/-(jsonmeta)  => jsonmeta.pure[ResultTIO]
    }
  } yield x

  private def fromJson(json: String): (String \/ SnapshotManifest) = Parse.decodeEither[SnapshotManifest](json)

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

  private def baseJsonObject(snapshotId: SnapshotId, currentVersion: SnapshotManifestVersion, date: Date, storeOrCommitId: (FeatureStoreId \&/ CommitId)): Json = {

    val f: Option[FeatureStoreId] = storeOrCommitId.a
    val c: Option[CommitId] = storeOrCommitId.b

    ("id" := snapshotId) ->: ("format_version" := currentVersion) ->: ("date" := date) ->: f.map("store_id" := _) ->?: c.map("commit_id" := _) ->?: jEmptyObject
  }

  // These are in series/7.1.x

  private def wrapThis[A, B](x: A): (A \&/ B) = This(x)

  private def wrapThat[A, B](x: B): (A \&/ B) = That(x)

  private def wrapBoth[A, B](x: A, y: B): (A \&/ B) = Both(x, y)

  // These dont seem to be in scalaz at all

  /**
   * Convert \&/ into a \/, in the Both case, the B (That) value is dropped
   * for cases where you need the A value but will need to make do with the B value if there
   * isn't one
   **/
  private def eitherThis[A, B](x: (A \&/ B)): (A \/ B) = x.fold(_.left, _.right, (x, _) => x.left[B])

  /**
   * Convert \&/ into a \/, in the Both case, the A (This) value is dropped
   * for cases where you need the B value but will need to make do with the A value if there
   * isn't one
   **/
  private def eitherThat[A, B](x: (A \&/ B)): (A \/ B) = x.fold(_.left, _.right, (_, y) => y.right[A])
}
