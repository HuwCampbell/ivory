package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import scalaz._

case class SnapshotExtractManifest(version: VersionManifest, commit: CommitId, snapshot: SnapshotId)

object SnapshotExtractManifest {
  def create(commit: CommitId, snapshot: SnapshotId): SnapshotExtractManifest =
    SnapshotExtractManifest(VersionManifest.current, commit, snapshot)

  def io(output: IvoryLocation): ManifestIO[SnapshotExtractManifest] =
    ManifestIO(output)

  implicit def SnapshotExtractManifestEqual: Equal[SnapshotExtractManifest] =
    Equal.equalA[SnapshotExtractManifest]

  implicit def SnapshotExtractManifestCodecJson: CodecJson[SnapshotExtractManifest] =
    CodecManifest("extract:snapshot", v => v.version -> Json(
      "commit" := v.commit
    , "snapshot" := v.snapshot
    ), (v, m) => for {
      commit <- m.get[CommitId]("commit")
      snapshot <- m.get[SnapshotId]("snapshot")
    } yield SnapshotExtractManifest(v, commit, snapshot))
}
