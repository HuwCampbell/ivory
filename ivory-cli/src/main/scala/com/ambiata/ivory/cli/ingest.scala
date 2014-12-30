package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.ingestion._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io._
import org.joda.time.DateTimeZone
import scalaz.{Name => _, _}, Scalaz._

object ingest extends IvoryApp {

  case class CliArguments(inputs: List[String], timezone: Option[DateTimeZone], optimal: BytesQuantity)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('i', "input")                action { (x, c) => c.copy(inputs = x :: c.inputs) }            required() text
      s"""Path to data to import, in the form FORMAT[|NAMESPACE]=PATH.
         |Supported formats are in the style FORM[:ESCAPING]:DELIM (ie 'sparse:delimited:psv' or 'sparse:thrift').
         |If NAMESPACE is set then the input path is expected to contain partitioned factsets.
       """.stripMargin
    opt[Long]('o', "optimal-input-chunk")    action { (x, c) => c.copy(optimal = x.bytes) }    text "Optimal size (in bytes) of input chunk.."
    opt[String]('z', "timezone")             action { (x, c) => c.copy(timezone = Some(DateTimeZone.forID(x)))   } optional() text
      "Optional timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney). " +
      "Defaults to the timezone specified on creation time of the Ivory repository."
  }

  val cmd = IvoryCmd.withCluster[CliArguments](parser,
      CliArguments(Nil, None, 256.mb),
      repo => cluster => configuration => c => for {
        inputs  <- IvoryT.fromRIO(RIO.fromDisjunctionString(
          c.inputs.traverseU(InputFormat.fromString).flatMap(_.traverseU {
            case (f, ns, i) => IvoryLocation.parseUri(i, configuration).map(il => (f, ns, il))
          })
        ))
        factset <- Ingest.ingestFacts(repo, cluster, inputs, c.timezone, c.optimal)
      } yield List(s"""Successfully imported '${c.inputs.mkString(", ")}' as $factset into '${repo}'"""))

}
