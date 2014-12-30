package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import scalaz._

case class VersionManifest(format: MetadataVersion, ivory: IvoryVersion)

object VersionManifest {
  def current: VersionManifest =
    VersionManifest(MetadataVersion.V2, IvoryVersion.get)

  implicit def VersionManifestEqual: Equal[VersionManifest] =
    Equal.equalA

  implicit def VersionManifestCodecJson: CodecJson[VersionManifest] =
    casecodec2(VersionManifest.apply, VersionManifest.unapply)("metadata", "ivory")
}


case class VersionManifestPeek(version: VersionManifest)

object VersionManifestPeek {
 implicit def VersionManifestPeekEqual: Equal[VersionManifestPeek] =
    Equal.equalA

  implicit def VersionManifestCodecJson: CodecJson[VersionManifestPeek] =
    casecodec1(VersionManifestPeek.apply, VersionManifestPeek.unapply)("version")
}
