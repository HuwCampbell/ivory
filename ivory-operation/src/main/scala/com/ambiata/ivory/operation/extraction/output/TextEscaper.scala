package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core.TextEscaping
import org.apache.hadoop.conf.Configuration

object TextEscaper {

  type Append = (StringBuilder, String) => Unit

  def fromConfiguration(configuration: Configuration, delimiter: Char): Append =
    TextEscaping.fromString(configuration.get(Keys.Escaped, TextEscaping.Delimited.render)).getOrElse(TextEscaping.Delimited) match {
      case TextEscaping.Escaped =>
        (sb, s) =>
          TextEscaping.escapeAppend(delimiter, s, sb)
      case TextEscaping.Delimited =>
        (sb, s) =>
          sb.append(s)
          ()
    }

  def toConfiguration(configuration: Configuration, escaping: TextEscaping): Unit =
    configuration.set(Keys.Escaped, escaping.render)

  object Keys {
    val Escaped = "ivory.extract.delimiter"
  }
}
