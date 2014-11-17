package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.FeatureStoreGlob
import com.ambiata.ivory.storage._

import scalaz._, Scalaz._, \&/._, effect.IO
import scala.math.{Ordering => SOrdering}
import argonaut._, Argonaut._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._

sealed trait SnapshotManifestVersion {
  val long: Long
}

object SnapshotManifestVersionLegacy extends SnapshotManifestVersion {
  val long: Long = 0
}

object SnapshotManifestVersionV1 extends SnapshotManifestVersion {
  val long: Long = 1
}

object SnapshotManifestVersion {

  implicit def SnapshotManifestVersionCodecJson: CodecJson[SnapshotManifestVersion] = CodecJson.derived(
    EncodeJson(_.long.asJson),
    DecodeJson.optionDecoder(_.as[Long].toOption.flatMap(fromLong), "SnapshotManifestVersion"))

  def fromLong(longVersion: Long): Option[SnapshotManifestVersion] = longVersion match {
    case SnapshotManifestVersionLegacy.long => SnapshotManifestVersionLegacy.some
    case SnapshotManifestVersionV1.long     => SnapshotManifestVersionV1.some
    case _                                  => none
  }
}

sealed trait SnapshotManifest {

  def fold[X](f: (legacy.SnapshotMeta => X), g: (NewSnapshotManifest => X)) : X = this match {
    case SnapshotManifestLegacy(lm) => f(lm)
    case SnapshotManifestNew(meta)  => g(meta)
  }

  def snapshotId: SnapshotId = fold(_.snapshotId, _.snapshotId)

  def formatVersion: SnapshotManifestVersion = fold(_ => SnapshotManifestVersionLegacy, _.formatVersion)

  def date: Date = fold(_.date, _.date)

  def storeOrCommitId : (FeatureStoreId \&/ CommitId) = fold(
    (lm: legacy.SnapshotMeta) => lm.commitId.cata(
      wrapBoth(lm.featureStoreId, _),
      wrapThis(lm.featureStoreId)),
    (meta: NewSnapshotManifest) => wrapThat(meta.commitId))

  def order(other: SnapshotManifest): Ordering = (this, other) match {
    case (SnapshotManifestLegacy(_), SnapshotManifestNew(_)) => Ordering.LT
    case (SnapshotManifestNew(_), SnapshotManifestLegacy(_)) => Ordering.GT
    case (SnapshotManifestLegacy(lm1), SnapshotManifestLegacy(lm2)) => lm1 order lm2
    case (SnapshotManifestNew(jm1), SnapshotManifestNew(jm2)) => jm1 order jm2
  }

  // These are in scalaz series/7.2.x

  private def wrapThis[A, B](x: A): (A \&/ B) = This(x)

  private def wrapThat[A, B](x: B): (A \&/ B) = That(x)

  private def wrapBoth[A, B](x: A, y: B): (A \&/ B) = Both(x, y)

}

case class SnapshotManifestLegacy(lm: legacy.SnapshotMeta) extends SnapshotManifest
case class SnapshotManifestNew(meta: NewSnapshotManifest) extends SnapshotManifest

object SnapshotManifest {

  def snapshotManifestLegacy(lm: legacy.SnapshotMeta): SnapshotManifest = new SnapshotManifestLegacy(lm)

  def snapshotManifestNew(meta: NewSnapshotManifest): SnapshotManifest = new SnapshotManifestNew(meta)

  def fromIdentifier(repo: Repository, id: SnapshotId): OptionT[ResultTIO, SnapshotManifest] = for {
    // try reading the JSON one first:
    jsonExists <- repo.store.exists(Repository.snapshot(id) / NewSnapshotManifest.metaKeyName).liftM[OptionT]

    x <- {
      if (jsonExists)
        NewSnapshotManifest.newManifestFromIdentifier(repo, id).map(snapshotManifestNew).liftM[OptionT]
      else
        OptionT.optionT(legacy.SnapshotMeta.fromIdentifier(repo, id)).map(snapshotManifestLegacy)
    }
  } yield x

  def getFeatureStoreId(repo: Repository, meta: SnapshotManifest): ResultTIO[FeatureStoreId] = meta.fold(
    (lm: legacy.SnapshotMeta) => lm.featureStoreId.pure[ResultTIO],
    (nm: NewSnapshotManifest) => NewSnapshotManifest.getFeatureStoreId(repo, nm))

  def featureStoreSnapshot(repo: Repository, meta: SnapshotManifest): ResultTIO[legacy.FeatureStoreSnapshot] = for {
    storeId <- getFeatureStoreId(repo, meta)
    store <- Metadata.featureStoreFromIvory(repo, storeId)
  } yield legacy.FeatureStoreSnapshot(meta.snapshotId, meta.date, store)

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
    /* This looks fairly weird on first glance, but basically we should be ignoring snapshots that are invalid or
       incomplete. In fact this is a totally normal situation (parallel or aborted snapshots). */
    sids <- ids.flatMap(sid => SnapshotId.parse(sid.name).toList).pure[ResultTIO].liftM[OptionT]
    metas <- sids.traverseU(fromIdentifier(repository, _).run).map(_.flatMap(_.toList)).liftM[OptionT]
    filtered = metas.filter(_.date isBeforeOrEqual date)
    _ = println("Candidate snapshots: ")
    _ = filtered.sorted.map(m => s"  - id: ${m.snapshotId}, date: ${m.date}.").foreach(println)
    meta <- OptionT.optionT[ResultTIO](filtered.sorted.lastOption.pure[ResultTIO])
    _ = println(s"Selected: ${meta.snapshotId}")
  } yield meta

  def latestWithStoreId(repo: Repository, date: Date, featureStoreId: FeatureStoreId): OptionT[ResultTIO, SnapshotManifest] = for {
    meta <- latestSnapshot(repo, date)
    metaFeatureId <- getFeatureStoreId(repo, meta).liftM[OptionT]
    x <- {
      if (metaFeatureId == featureStoreId)
        OptionT.some[ResultTIO, SnapshotManifest](meta)
      else
        OptionT.none[ResultTIO, SnapshotManifest]
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
        OptionT.none[ResultTIO, SnapshotManifest]
    }

  } yield x

  // Instances

  implicit def SnapshotManifestOrder: Order[SnapshotManifest] =
    Order.order(_ order _)

  implicit def SnapshotManifestSOrdering: SOrdering[SnapshotManifest] =
    SnapshotManifestOrder.toScalaOrdering

  // helpers
  private def checkForNewerFeatures(repo: Repository, metaFeatureId: FeatureStoreId, store: FeatureStore, beginDate: Date, endDate: Date): ResultTIO[Boolean] = {
    if (metaFeatureId == store.id) {
      if (beginDate == endDate)
        true.pure[ResultTIO]
      else
        FeatureStoreGlob.between(repo, store, beginDate, endDate).map(_.partitions.isEmpty)
    } else false.pure[ResultTIO]
  }


}

/**
 * Representation of Snapshot metadata stored on disk
 */
case class NewSnapshotManifest(
  snapshotId: SnapshotId,
  formatVersion: SnapshotManifestVersion,
  date: Date,
  commitId: CommitId) {

  // version shouldn't be relevent to ordering.
  def order(other: NewSnapshotManifest): Ordering = (snapshotId, date, commitId).?|?((other.snapshotId, other.date, other.commitId))

}

object NewSnapshotManifest {

  // FIXME: Remove the `unsafe` after the KeyName macros accessibility has been fixed.
  val metaKeyName = KeyName.unsafe(".metadata.json")

  private val allocated = KeyName.unsafe(".allocated")

  // exported functions:

  def newSnapshotMeta(
    snapshotId: SnapshotId,
    date: Date,
    commitId: CommitId) = NewSnapshotManifest(snapshotId, SnapshotManifestVersionV1, date, commitId)

  def createSnapshotManifest(repo: Repository, date: Date): ResultTIO[NewSnapshotManifest] = for {
    snapshotId <- allocateId(repo)
    commitId <- Metadata.findOrCreateLatestCommitId(repo)
  } yield newSnapshotMeta(snapshotId, date, commitId)

  def getFeatureStoreId(repo: Repository, meta: NewSnapshotManifest): ResultTIO[FeatureStoreId] = Metadata.commitFromIvory(repo, meta.commitId).map(_.featureStoreId)

  def featureStoreSnapshot(repo: Repository, meta: NewSnapshotManifest): ResultTIO[legacy.FeatureStoreSnapshot] = for {
    storeId <- getFeatureStoreId(repo, meta)
    store <- Metadata.featureStoreFromIvory(repo, storeId)
  } yield legacy.FeatureStoreSnapshot(meta.snapshotId, meta.date, store)

  def newManifestFromIdentifier(repo: Repository, id: SnapshotId): ResultTIO[NewSnapshotManifest] = for {
    json <- repo.store.utf8.read(Repository.snapshot(id) / NewSnapshotManifest.metaKeyName)
    x <- fromJson(json) match {
      case -\/(msg)       => ResultT.fail[IO, NewSnapshotManifest]("failed to parse Snapshot manifest: " ++ msg)
      case \/-(jsonmeta)  => jsonmeta.pure[ResultTIO]
    }
  } yield x

  def save(repository: Repository, meta: NewSnapshotManifest): ResultTIO[Unit] = repository.store.utf8.write(
    Repository.snapshot(meta.snapshotId) / NewSnapshotManifest.metaKeyName,
    meta.asJson.nospaces)

  // instances

  implicit def NewSnapshotManifestOrder: Order[NewSnapshotManifest] = Order.order(_ order _)

  implicit def NewSnapshotManifestSOrdering: SOrdering[NewSnapshotManifest] =
    NewSnapshotManifestOrder.toScalaOrdering

  implicit def NewSnapshotManifestCodecJson : CodecJson[NewSnapshotManifest] = CodecJson(
    toJsonObject,
    ((c: HCursor) => for {
      id <- (c --\ "id").as[SnapshotId]
      version <- (c --\ "format_version").as[SnapshotManifestVersion]
      date <- (c --\ "date").as[Date]
      commitId <- (c --\ "commit_id").as[CommitId]
    } yield NewSnapshotManifest(id, version, date, commitId)))

  // helpers

  private def fromJson(json: String): (String \/ NewSnapshotManifest) = Parse.decodeEither[NewSnapshotManifest](json)

  /**
   * create a new Snapshot id by create a new .allocated sub-directory
   * with the latest available identifier + 1
   */
  private def allocateId(repo: Repository): ResultTIO[SnapshotId] =
    IdentifierStorage.write(repo, Repository.snapshots, allocated, scodec.bits.ByteVector.empty).map(SnapshotId.apply)

  private def toJsonObject(meta: NewSnapshotManifest): Json = {
    ("id" := meta.snapshotId) ->: ("format_version" := meta.formatVersion) ->: ("date" := meta.date) ->: ("commit_id" := meta.commitId) ->: jEmptyObject
  }

}
