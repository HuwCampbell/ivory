package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed trait EncodedAs {
  import EncodedAs._

  def render: String = this match {
    case Delimited =>
      "delimited"
    case Escaped =>
      "escaped"
  }
}

object EncodedAs {
  object Delimited extends EncodedAs
  object Escaped extends EncodedAs

  def fromString(s: String): Option[EncodedAs] =
    s match {
      case "delimited" => Delimited.some
      case "escaped" => Escaped.some
      case _ => none
    }

  def unapply(s: String): Option[EncodedAs] =
    fromString(s)

  implicit def EncodedAsEqual: Equal[EncodedAs] =
    Equal.equalA[EncodedAs]
}
