package com.ambiata.ivory.core

import scalaz._, Scalaz._

sealed abstract class Delimiter(val name: String, val character: Char) {
  def render: String =
    name
}

object Delimiter {
  case object Psv extends Delimiter("psv", '|')
  case object Csv extends Delimiter("csv", ',')
  case object Tsv extends Delimiter("tsv", '\t')

  implicit def DelimiterEqual: Equal[Delimiter] =
    Equal.equalA[Delimiter]

  def unapply(s: String): Option[Delimiter] =
    fromString(s)

  def fromString(s: String): Option[Delimiter] = s match {
    case "psv" =>
      Psv.some
    case "csv" =>
      Csv.some
    case "tsv" =>
      Tsv.some
    case _ =>
      none
  }
}
