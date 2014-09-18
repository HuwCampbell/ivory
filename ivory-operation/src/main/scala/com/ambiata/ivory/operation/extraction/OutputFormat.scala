package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._

sealed trait OutputFormat
case object PivotFormat extends OutputFormat

object OutputFormat {

  def fromString(s: String): Option[OutputFormat] = s.toLowerCase match {
    case "dense:psv" => Some(PivotFormat)
    case _           => None
  }
}

case class OutputFormats(outputs: List[(OutputFormat, ReferenceIO)], delim: Char, missingValue: String)
