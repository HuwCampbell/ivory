package com.ambiata.ivory.core

import argonaut._, Argonaut._
import scalaz._, Scalaz._

sealed trait FileFormat {
  import FileFormat._

  def fold[X](
    text: (Delimiter, TextEscaping) => X
  , thrift: => X
  ): X = this match {
    case Text(delimiter, encoding) =>
      text(delimiter, encoding)
    case Thrift =>
      thrift
  }

  def isText: Boolean =
    fold((_, _) => true, false)

  def isThrift: Boolean =
    fold((_, _) => false, true)

  def render: String =
    fold(
      (delimiter, escaping) => s"${escaping.render}:${delimiter.render}"
      , "thrift"
    )

}

object FileFormat {
  case class Text(delimiter: Delimiter, escaping: TextEscaping) extends FileFormat
  case object Thrift extends FileFormat

  def text(delimiter: Delimiter, escaping: TextEscaping): FileFormat =
    Text(delimiter, escaping)

  def thrift: FileFormat =
    Thrift

  def fromString(s: String): Option[FileFormat] =
    s.split(":").toList match {
      case TextEscaping(escaping) :: Delimiter(delimiter) :: Nil =>
        Text(delimiter, escaping).some
      case Delimiter(delimiter) :: Nil =>
        Text(delimiter, TextEscaping.Delimited).some
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
