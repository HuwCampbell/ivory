package com.ambiata.ivory.core

import argonaut._
import scalaz._

/** This represents the version lock on all metadata stored in ivory, any
    change that will break existing clients _must_ update this version,
    to ensure forward compatibility constraints can be checked and prevent
    older ivory installs from ignoring important information or checks. */
sealed trait MetadataVersion {
}

object MetadataVersion {
  /** An unkown, probably future version. */
  case class Unknown(value: String) extends MetadataVersion

  /** The lack of versioning. */
  case object V0 extends MetadataVersion

  /** The initial json format metadata. */
  case object V1 extends MetadataVersion

  /** Added 'size' to factset and snapshot metadata to assist with planning. */
  case object V2 extends MetadataVersion

  /** Fixing snapshot manifest which were accidentally missing in original V1/V2 */
  case object V3 extends MetadataVersion

  implicit def MetadataVersionEqual: Equal[MetadataVersion] =
    Equal.equalA[MetadataVersion]

  implicit def MetadataVersionCodecJson: CodecJson[MetadataVersion] =
    ArgonautPlus.codecEnum("MetadataVersion", {
      case V0 => "v0"
      case V1 => "v1"
      case V2 => "v2"
      case V3 => "v3"
      case Unknown(x) => x
    }, {
      case "v0" => V0
      case "v1" => V1
      case "v2" => V2
      case "v3" => V3
      case x => Unknown(x)
    })

  def previousVersions = List(V0, V1, V2)
  def latestVersion = V3
}
