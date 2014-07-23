package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.ingest.EavtTextImporter
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.fact.Namespaces
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.io._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory
import org.joda.time.DateTimeZone
import MemoryConversions._
import scalaz.{DList => _, Scalaz}
import Scalaz._
import MemoryConversions._

object ingest extends IvoryApp {

  case class CliArguments(repo: String, input: String, timezone: DateTimeZone, optimal: BytesQuantity)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('r', "repo")                 action { (x, c) => c.copy(repo = x) }       required() text "Path to an ivory repository."
    opt[String]('i', "input")                action { (x, c) => c.copy(input = x) }      required() text "Path to data to import."
    opt[Long]('o', "optimal-input-chunk")    action { (x, c) => c.copy(optimal = x.bytes) }      text "Optimal size (in bytes) of input chunk.."
    opt[String]('z', "timezone")             action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  type Namespace = String

  def cmd = IvoryCmd[CliArguments](parser,
      CliArguments("", "", DateTimeZone.getDefault, 256.mb),
      ScoobiCmd(configuration => c => {
      val res = onHdfs(new Path(c.repo), new Path(c.input), c.timezone, c.optimal, Codec())
      res.run(configuration).map {
        case f => List(s"Successfully imported '${c.input}' as ${f} into '${c.repo}'")
      }
    }))

  def onHdfs(repo: Path, input: Path, timezone: DateTimeZone, optimal: BytesQuantity, codec: Option[CompressionCodec]): ScoobiAction[Factset] =
    fatrepo.ImportWorkflow.onHdfs(repo, importFeed(input, optimal, codec), timezone)

  def importFeed(input: Path, optimal: BytesQuantity, codec: Option[CompressionCodec])(repo: HdfsRepository, factset: Factset, errorPath: Path, timezone: DateTimeZone): ScoobiAction[Unit] = for {
    dict <- ScoobiAction.fromResultTIO(dictionaryFromIvory(repo))
    list <- ScoobiAction.fromHdfs(Namespaces.namespaceSizes(input))
    conf <- ScoobiAction.scoobiConfiguration
    _    <- EavtTextImporter.onHdfs(repo, dict, factset, list.map(_._1.toString), input, errorPath, timezone, list.toMap.mapKeys(_.toString).toList, optimal, codec)
  } yield ()

}
