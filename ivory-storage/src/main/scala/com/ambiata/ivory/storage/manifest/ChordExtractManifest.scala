package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import scalaz._


case class ChordExtractManifest(version: VersionManifest, commit: CommitId)

object ChordExtractManifest {
  def create(commit: CommitId): ChordExtractManifest =
    ChordExtractManifest(VersionManifest.current, commit)

  def io(output: IvoryLocation): ManifestIO[ChordExtractManifest] =
    ManifestIO(output)

  implicit def ChordExtractManifestEqual: Equal[ChordExtractManifest] =
    Equal.equalA

  implicit def ChordExtractManifestCodecJson: CodecJson[ChordExtractManifest] =
    CodecManifest("extract:chord", v => v.version -> Json(
      "commit" := v.commit
    ), (v, m) => for {
      commit <- m.get[CommitId]("commit")
    } yield ChordExtractManifest(v, commit))
}
