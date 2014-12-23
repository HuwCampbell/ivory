package com.ambiata.ivory.core

import argonaut._
import scalaz._

/** This represents the version of the on-disk data that makes up a factset. */
sealed trait FactsetFormat

object FactsetFormat {
  /** V1 is a sequence file, with a null-key and a "thrift-fact" (i.e. no namespace / date component)
      value stored as bytes value, and the namespace / date encoded in the partition. */
  case object V1 extends FactsetFormat

  /** V2 is a sequence file, with a null-key and a "thrift-fact" (i.e. no namespace / date component)
      value stored as bytes value, and the namespace / date encoded in the partition.
      NOTE this is identical to V1 and was used to force out the potential to read
      multiple factset versions. */
  case object V2 extends FactsetFormat

  /* NOTE Don't forget the arbitrary if you add a format. */

  implicit def FactsetFormatEqual: Equal[FactsetFormat] =
    Equal.equalA[FactsetFormat]

  implicit def FactsetFormatCodecJson: CodecJson[FactsetFormat] =
    ArgonautPlus.codecEnum("FactsetFormat", {
      case V1 => "v1"
      case V2 => "v2"
    }, {
      case "v1" => V1
      case "v2" => V2
    })
}
