package com.ambiata.ivory.core

import argonaut._
import scalaz._


/** This represents the version of the on-disk data that makes up an internal snapshot. */
sealed trait SnapshotFormat

object SnapshotFormat {
  /** V1 is a sequence file, with a null-key and a "namespaced-thrift-fact" value stored as
      bytes value with no partitioning. */
  case object V1 extends SnapshotFormat

  /* NOTE Don't forget the arbitrary if you add a format. */

  implicit def SnapshotFormatEqual: Equal[SnapshotFormat] =
    Equal.equalA[SnapshotFormat]

  implicit def SnapshotFormatCodecJson: CodecJson[SnapshotFormat] =
    ArgonautPlus.codecEnum("SnapshotFormat", {
      case V1 => "v1"
    }, {
      case "v1" => V1
    })
}
