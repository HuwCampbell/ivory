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
import com.ambiata.mundane.control._
import org.apache.hadoop.fs.Path
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.alien.hdfs._

import org.apache.hadoop.io.compress._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.commons.logging.LogFactory
import org.joda.time.DateTimeZone
import MemoryConversions._

import scalaz.{DList => _, _}, Scalaz._, effect._

object ingest extends IvoryApp {

  case class CliArguments(repo: String, input: String, namespace: Option[String], timezone: DateTimeZone, optimal: BytesQuantity)

  val parser = new scopt.OptionParser[CliArguments]("ingest") {
    head("""
         |Fact ingestion pipeline.
         |
         |This will import a set of facts using the latest dictionary.
         |
         |""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('r', "repo")                 action { (x, c) => c.copy(repo = x) }             required() text "Path to an ivory repository."
    opt[String]('i', "input")                action { (x, c) => c.copy(input = x) }            required() text "Path to data to import."
    opt[String]('n', "namespace")            action { (x, c) => c.copy(namespace = Some(x)) }  optional() text
      "Namespace to import. If set the input path is expected to contain partitioned factsets"
    opt[Long]('o', "optimal-input-chunk")    action { (x, c) => c.copy(optimal = x.bytes) }    text "Optimal size (in bytes) of input chunk.."
    opt[String]('z', "timezone")             action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  type Namespace = String

  def cmd = IvoryCmd[CliArguments](parser,
      CliArguments("", "", None, DateTimeZone.getDefault, 256.mb),
      ScoobiRunner(configuration => c => for {
        repo     <- Repository.fromUriResultTIO(c.repo, configuration)
        inputRef <- Reference.fromUriResultTIO(c.input, configuration)
        factset  <- run(repo, inputRef, c.namespace, c.timezone, c.optimal, Codec())
      } yield List(s"Successfully imported '${c.input}' as ${factset} into '${c.repo}'")))

  def run(repo: Repository, input: ReferenceIO, namespace: Option[String], timezone: DateTimeZone, optimal: BytesQuantity, codec: Option[CompressionCodec]): ResultTIO[Factset] =
    fatrepo.ImportWorkflow.onStore(repo, importFeed(input, namespace, optimal, codec), timezone)

  def importFeed(input: ReferenceIO, namespace: Option[String], optimal: BytesQuantity, codec: Option[CompressionCodec])(repo: Repository, factset: Factset, errorRef: ReferenceIO, timezone: DateTimeZone): ResultTIO[Unit] = for {
    conf <- repo match {
      case HdfsRepository(_, c, _) => ResultT.ok[IO, Configuration](c)
      case _                       => ResultT.fail[IO, Configuration]("Currently only support HDFS repository")
    }
    dict <- dictionaryFromIvory(repo)
    path <- Reference.hdfsPath(input)
    list <- Namespaces.namespaceSizes(path, namespace).run(conf)
    _    <- EavtTextImporter.onStore(repo, dict, factset, list.map(_._1), input, errorRef, timezone, list.toMap.mapKeys(_.toString).toList, optimal, codec)
  } yield ()
}
