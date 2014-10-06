package com.ambiata.ivory.cli

import com.ambiata.ivory.core.Name._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.ingestion._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io._
import org.joda.time.DateTimeZone

import scalaz.{Name => _, DList => _}

object ingest extends IvoryApp {

  case class CliArguments(input: String, namespace: Option[Name], timezone: DateTimeZone,
                          optimal: BytesQuantity, format: Format)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('i', "input")                action { (x, c) => c.copy(input = x) }            required() text "Path to data to import."
    opt[String]('n', "namespace")            action { (x, c) => c.copy(namespace = nameFromString(x)) }  optional() text
      "Namespace to import. If set the input path is expected to contain partitioned factsets"
    opt[Long]('o', "optimal-input-chunk")    action { (x, c) => c.copy(optimal = x.bytes) }    text "Optimal size (in bytes) of input chunk.."
    opt[String]('f', "format")               action { (x, c) => c.copy(format = Format.parse(x)) }        text
      "Optional format for ingestion [text|thrift], defaults to 'text'"
    opt[String]('z', "timezone")             action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  type Namespace = String

  val cmd = IvoryCmd.withRepo[CliArguments](parser,
      CliArguments("", None, DateTimeZone.getDefault, 256.mb, TextFormat),
      repo => configuration => c => for {
        input   <- IvoryLocation.fromUri(c.input, configuration)
        factset <- Ingest.ingestFacts(repo, input, c.namespace, c.timezone, c.optimal, c.format)
      } yield List(s"Successfully imported '${c.input}' as $factset into '${repo}'"))
}
