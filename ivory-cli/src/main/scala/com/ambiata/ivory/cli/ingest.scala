package com.ambiata.ivory.cli

import com.ambiata.ivory.cli.PirateReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.ingestion._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io._

import org.joda.time.DateTimeZone

import pirate._, Pirate._

import scalaz._, Scalaz._

object ingest extends IvoryApp {

  case class CliArguments(inputs: List[String], timezone: Option[DateTimeZone], optimal: BytesQuantity,
                          cluster: IvoryConfiguration => Cluster)

  val parser = Command(
    "ingest"
  , Some("""
    |Fact ingestion pipeline.
    |
    |This will import a set of facts using the latest dictionary.
    |
    |""".stripMargin)
  , CliArguments |*| (
    flag[String](both('i', "input"), description(s"""
      |Path to data to import, in the form FORMAT[|NAMESPACE]=PATH.
      |Supported formats are in the style FORM[:ESCAPING]:DELIM (ie 'sparse:delimited:psv' or 'sparse:thrift').
      |If NAMESPACE is set then the input path is expected to contain partitioned factsets.
      """.stripMargin)).some
  , flag[DateTimeZone](both('z', "timezone"), description("""
      |Optional timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney).
      |Defaults to the timezone specified on creation time of the Ivory repository.
      |""".stripMargin)).option
  , flag[BytesQuantity](both('o', "optimal-input-chunk"), description("Optimal size (in bytes) of input chunk.")).default(256.mb)
  , IvoryCmd.cluster
  ))

  val cmd = IvoryCmd.withRepo[CliArguments](parser,
      repo => configuration => c => for {
        inputs  <- IvoryT.fromRIO(RIO.fromDisjunctionString(
          c.inputs.traverseU(InputFormat.fromString).flatMap(_.traverseU {
            case (f, ns, i) => IvoryLocation.parseUri(i, configuration).map(il => (f, ns, il))
          })
        ))
        _ <- IvoryT.fromRIO(inputs.traverseU {
          case (_, ns, i) => ns.cata(_ => RIO.unit,
            IvoryLocation.isDirectory(i).flatMap(RIO.unless(_, RIO.fail(s"Invalid file ${i.show} for ingesting namespaces - must be a directory"))))
        })
        factset <- Ingest.ingestFacts(repo, c.cluster(configuration), inputs, c.timezone, c.optimal)
      } yield List(s"""Successfully imported '${c.inputs.mkString(", ")}' as $factset into '${repo}'"""))

}
