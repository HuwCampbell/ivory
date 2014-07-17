package com.ambiata.ivory.storage.metadata

sealed abstract class DictionaryVersion(override val toString: String)
case object DictionaryVersionOne extends DictionaryVersion("1")

object DictionaryVersion {
  def fromString(str: String): Option[DictionaryVersion] = str match {
    case "1" => Some(DictionaryVersionOne)
    case _   => None
  }
}
