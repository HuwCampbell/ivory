package com.ambiata.ivory.core

import argonaut._, Argonaut._
import scalaz._, Scalaz._

case class OutputFormat(form: Form, format: FileFormat) {

  def render: String =
    s"${form.render}:${format.render}"
}

object OutputFormat {

  def fromString(s: String): Option[OutputFormat] =
    s.split(":", 2).toList match {
      case form :: format :: Nil =>
        (Form.fromString(form) |@| FileFormat.fromString(format))(OutputFormat.apply)
      case _ =>
        none
    }

  implicit def OutputFormatEqual: Equal[OutputFormat] =
    Equal.equalA[OutputFormat]

  implicit def OutputFormatEncodeJson: EncodeJson[OutputFormat] =
    EncodeJson(_.render.asJson)

  implicit def OutputFormatDecodeJson: DecodeJson[OutputFormat] =
    DecodeJson.optionDecoder(_.string >>= fromString, "OutputFormat")
}
