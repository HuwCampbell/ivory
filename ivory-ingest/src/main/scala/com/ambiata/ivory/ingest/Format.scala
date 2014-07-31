package com.ambiata.ivory.ingest

sealed trait Format
case object TextFormat extends Format
case object ThriftFormat extends Format

object Format {

  def parse(s: String): Format = s.toLowerCase match {
    case "thrift" => ThriftFormat
    case _        => TextFormat
  }
}
