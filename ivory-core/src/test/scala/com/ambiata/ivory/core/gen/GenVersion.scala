package com.ambiata.ivory.core

import org.scalacheck._

object GenVersion {
  def snapshot: Gen[SnapshotFormat] =
    Gen.oneOf(SnapshotFormat.V1, SnapshotFormat.V2)

  def factset: Gen[FactsetFormat] =
    Gen.oneOf(FactsetFormat.V1, FactsetFormat.V2)

  def metadata: Gen[MetadataVersion] =
    Gen.oneOf(MetadataVersion.latestVersion :: MetadataVersion.previousVersions)
}
