package com.ambiata.ivory.operation.ingestion

import scalaz._, Scalaz._

sealed trait Format
case object TextDelimitedFormat extends Format
case object TextEscapedFormat extends Format
case object ThriftFormat extends Format

object Format {

  val formats = Map(
    "text"           -> TextDelimitedFormat,
    "text:delimited" -> TextDelimitedFormat,
    "text:escaped"   -> TextEscapedFormat,
    "thrift"         -> ThriftFormat
  )

  def parse(s: String): String \/ Format =
    formats.get(s.toLowerCase).toRightDisjunction(s"Unknown format '$s'")
}
