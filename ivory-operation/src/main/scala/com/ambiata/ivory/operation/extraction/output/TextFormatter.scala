package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._
import org.apache.hadoop.conf.Configuration

object TextFormatter {

  def fromConfiguration(configuration: Configuration, missing: String): Value => String =
    TextFormat.fromString(configuration.get(Keys.Formatter, TextEscaping.Delimited.render)).getOrElse(TextEscaping.Delimited) match {
      case TextFormat.Deprecated =>
        value => Value.text.toStringWithStruct(value, missing)
      case TextFormat.Json =>
        value => Value.json.toStringWithStruct(value, missing)
    }

  def toConfiguration(configuration: Configuration, format: TextFormat): Unit =
    configuration.set(Keys.Formatter, format.render)

  object Keys {
    val Formatter = "ivory.extract.formatter"
  }
}
