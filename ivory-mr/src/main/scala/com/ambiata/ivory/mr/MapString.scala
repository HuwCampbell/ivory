package com.ambiata.ivory.mr

/**
 * Very simple helper for converting a (relatively small) [[Map]] to/from a string for Hadoop.
 *
 * For anything medium/large please consider [[com.ambiata.poacher.mr.ThriftCache]] instead.
 */
object MapString {

  // Characters that are very unlikely to be used by consumers
  val sep1 = "\u9999"
  val sep2 = "\u9998"

  def render(map: Map[String, String]): String =
    map.map {
      case (root, delim) => root + sep2 + delim
    }.mkString(sep1)

  def fromString(s: String): Map[String, String] =
    if (!s.isEmpty) s.split(sep1, -1).map { p => val ps = p.split(sep2, 2); ps(0) -> ps(1)}.toMap else Map.empty
}
