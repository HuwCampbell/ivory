package com.ambiata.ivory.storage.fact

import scalaz._, Scalaz._

// FIX delete, use core.FactsetFormat.
sealed abstract class FactsetVersion(val toByte: Byte) {
  override def toString() = toByte.toString
}
case object FactsetVersionOne extends FactsetVersion(1)
case object FactsetVersionTwo extends FactsetVersion(2)

object FactsetVersion {
  def fromByte(s: Byte): Option[FactsetVersion] = s match {
    case 1 => Some(FactsetVersionOne)
    case 2 => Some(FactsetVersionTwo)
    case _ => None
  }

  def fromString(str: String): Option[FactsetVersion] =
    str.parseByte.toOption.flatMap(fromByte)

  def fromStringOrLatest(str: String): FactsetVersion =
    fromString(str).getOrElse(latest)

  val latest = FactsetVersionTwo
}
