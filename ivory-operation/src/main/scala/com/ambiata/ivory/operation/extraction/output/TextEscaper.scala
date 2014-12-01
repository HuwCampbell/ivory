package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core.TextEscaping
import org.apache.hadoop.conf.Configuration

object TextEscaper {

  type Append = (StringBuilder, String) => Unit

  def fromConfiguration(configuration: Configuration, delimiter: Char): Append =
    if (configuration.getBoolean(Keys.Escaped, false)) {
      (sb, s) =>
        TextEscaping.escapeAppend(delimiter, s, sb)
    } else {
      (sb, s) =>
        sb.append(s)
        ()
    }

  def toConfiguration(configuration: Configuration, escaped: Boolean): Unit =
    configuration.setBoolean(Keys.Escaped, escaped)

  object Keys {
    val Escaped = "ivory.extract.delimiter"
  }
}
