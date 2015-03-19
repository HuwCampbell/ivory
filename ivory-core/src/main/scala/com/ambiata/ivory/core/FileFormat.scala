package com.ambiata.ivory.core

import argonaut._, Argonaut._
import scalaz._, Scalaz._

sealed trait FileFormat {
  import FileFormat._

  def fold[X](
    text: (Delimiter, TextEscaping, TextFormat) => X
  , thrift: => X
  ): X = this match {
    case Text(delimiter, encoding, format) =>
      text(delimiter, encoding, format)
    case Thrift =>
      thrift
  }

  def isText: Boolean =
    fold((_, _, _) => true, false)

  def isThrift: Boolean =
    fold((_, _, _) => false, true)

  def render: String =
    fold(
      (delimiter, escaping, format) => s"${escaping.render}:${delimiter.render}:${format.render}"
      , "thrift"
    )

}

object FileFormat {
  case class Text(delimiter: Delimiter, escaping: TextEscaping, format: TextFormat) extends FileFormat
  case object Thrift extends FileFormat

  def text(delimiter: Delimiter, escaping: TextEscaping, format: TextFormat): FileFormat =
    Text(delimiter, escaping, format)

  def thrift: FileFormat =
    Thrift

  def fromString(s: String): Option[FileFormat] =
    s.split(":").toList match {
      case TextEscaping(escaping) :: Delimiter(delimiter) :: TextFormat(format) :: Nil =>
        Text(delimiter, escaping, format).some
      case TextEscaping(escaping) :: Delimiter(delimiter) :: Nil =>
        Text(delimiter, escaping, TextFormat.Json).some
      case Delimiter(delimiter) :: Nil =>
        Text(delimiter, TextEscaping.Delimited, TextFormat.Json).some
      case "thrift" :: Nil =>
        Thrift.some
      case _ =>
        none
    }

  implicit def FileFormatEqual: Equal[FileFormat] =
    Equal.equalA[FileFormat]

  implicit def FileFormatEncodeJson: EncodeJson[FileFormat] =
    EncodeJson(_.render.asJson)

  implicit def FileFormatDecodeJson: DecodeJson[FileFormat] =
    DecodeJson.optionDecoder(_.string >>= fromString, "FileFormat")
}
