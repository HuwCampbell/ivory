package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import scalaz._, Scalaz._

case class SnapshotManifest(version: VersionManifest, storeOrCommit: FeatureStoreId \/ CommitId, snapshot: SnapshotId, format: SnapshotFormat, date: Date)

object SnapshotManifest {
  def create(commit: CommitId, snapshot: SnapshotId, format: SnapshotFormat, date: Date): SnapshotManifest =
    SnapshotManifest(VersionManifest.current, commit.right, snapshot, format, date)

  def createLatest(commit: CommitId, snapshot: SnapshotId, date: Date): SnapshotManifest =
    create(commit, snapshot, SnapshotFormat.V1, date)

  def createDeprecated(storeOrCommit: FeatureStoreId \/ CommitId, snapshot: SnapshotId, format: SnapshotFormat, date: Date): SnapshotManifest =
    SnapshotManifest(VersionManifest.current, storeOrCommit, snapshot, format, date)

  def io(repository: Repository, id: SnapshotId): ManifestIO[SnapshotManifest] =
    ManifestIO(repository.toIvoryLocation(Repository.snapshot(id)))

  implicit def SnapshotManifestEqual: Equal[SnapshotManifest] =
    Equal.equalA[SnapshotManifest]

  implicit def SnapshotManifestCodecJson: CodecJson[SnapshotManifest] =
    CodecManifest("snapshot", v => v.version -> Json(
      "commit" := v.storeOrCommit.toOption
    , "store" := v.storeOrCommit.swap.toOption
    , "snapshot" := v.snapshot
    , "format" := v.format
    , "date" := v.date
    ), (v, m) => for {
      commit <- m.get[Option[CommitId]]("commit").map(_.map(_.right[FeatureStoreId]))
      store <- m.get[Option[FeatureStoreId]]("store").map(_.map(_.left[CommitId]))
      sc <- commit.orElse(store).cata(DecodeResult.ok, DecodeResult.fail("Missing 'commit' or 'store'", m.history))
      snapshot <- m.get[SnapshotId]("snapshot")
      format <- m.get[SnapshotFormat]("format")
      date <- m.get[Date]("date")
    } yield SnapshotManifest(v, sc, snapshot, format, date))
}
