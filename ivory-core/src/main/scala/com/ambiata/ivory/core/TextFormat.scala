package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed trait TextFormat {
  import TextFormat._

  def fold[X](
    deprecated: => X
  , json: => X
  ): X = this match {
    case Deprecated =>
      deprecated
    case Json =>
      json
  }

  def render: String =
    fold("deprecated", "json")
}

object TextFormat {

  object Deprecated extends TextFormat
  object Json extends TextFormat

  def fromString(s: String): Option[TextFormat] =
    s match {
      case "deprecated" => Deprecated.some
      case "json" => Json.some
      case _ => none
    }

  def unapply(s: String): Option[TextFormat] =
    fromString(s)

  implicit def TextFormatEqual: Equal[TextFormat] =
    Equal.equalA[TextFormat]
}
