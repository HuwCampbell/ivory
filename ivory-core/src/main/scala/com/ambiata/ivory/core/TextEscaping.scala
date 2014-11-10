package com.ambiata.ivory.core

import scalaz.Equal
import scalaz.Scalaz._

sealed trait TextEscaping {
  import TextEscaping._

  def render: String = this match {
    case Delimited =>
      "delimited"
    case Escaped =>
      "escaped"
  }
}

object TextEscaping {

  object Delimited extends TextEscaping
  object Escaped extends TextEscaping

  def fromString(s: String): Option[TextEscaping] =
    s match {
      case "delimited" => Delimited.some
      case "escaped" => Escaped.some
      case _ => none
    }

  def unapply(s: String): Option[TextEscaping] =
    fromString(s)

  implicit def TextEscapingEqual: Equal[TextEscaping] =
    Equal.equalA[TextEscaping]

  val escapeChar = '\\'

  def escape(delim: Char, s: String): String = {
    // We want enough spare room for any escaped characters
    val b = new StringBuilder((s.length * 1.2).toInt)
    escapeAppend(delim, s, b)
    b.toString
  }

  def escapeAppend(delim: Char, s: String, b: StringBuilder): Unit = {
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (c == '\n') {
        b.append(escapeChar)
        b.append('n')
      } else if (c == '\r') {
        b.append(escapeChar)
        b.append('r')
      } else {
        if (c == delim || c == '\\') {
          b.append(escapeChar)
        }
        b.append(c)
      }
      i += 1
    }
  }

  def mkString(delim: Char, l: List[String]): String =
    if (l.isEmpty) ""
    else {
      val b = new StringBuilder((l.map(_.length).sum * 1.2).toInt)
      l.foreach { s => escapeAppend(delim, s, b); b.append(delim)}
      b.setLength(b.length - 1)
      b.toString
    }

  def split(delim: Char, s: String): List[String] = {
    val l = List.newBuilder[String]
    var i = 0
    val b = new StringBuilder(s.length)
    var lastCharWasDelim = false
    while (i < s.length) {
      lastCharWasDelim = false
      val c = s.charAt(i)
      if (c == '\\') {
        i += 1
        if (i < s.length) {
          val c2 = s.charAt(i)
          if (c2 == 'n') {
            b.append('\n')
          } else if (c2 == 'r') {
            b.append('\r')
          } else if (c2 == delim || c2 == '\\') {
            b.append(c2)
          } else {
            // Otherwise keep both characters
            b.append(c)
            b.append(c2)
          }
        } else {
          b.append(c)
        }
      } else if (c == delim) {
        l += b.toString
        b.clear
        lastCharWasDelim = true
      } else {
        b.append(c)
      }
      i += 1
    }
    if (b.nonEmpty || lastCharWasDelim) {
      l += b.toString
    }
    l.result
  }
}
