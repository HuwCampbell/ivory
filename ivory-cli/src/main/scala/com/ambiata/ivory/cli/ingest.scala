package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.ingestion._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact.Namespaces
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.ivory.core.Name._
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._
import com.ambiata.ivory.storage.store._
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTimeZone
import MemoryConversions._

import scalaz.{Name => _, DList => _, _}, Scalaz._, effect._

object ingest extends IvoryApp {

  case class CliArguments(repo: String, input: String, namespace: Option[Name], timezone: DateTimeZone,
                          optimal: BytesQuantity, format: Format)

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
    opt[String]('n', "namespace")            action { (x, c) => c.copy(namespace = nameFromString(x)) }  optional() text
      "Namespace to import. If set the input path is expected to contain partitioned factsets"
    opt[Long]('o', "optimal-input-chunk")    action { (x, c) => c.copy(optimal = x.bytes) }    text "Optimal size (in bytes) of input chunk.."
    opt[String]('f', "format")               action { (x, c) => c.copy(format = Format.parse(x)) }        text
      "Optional format for ingestion [text|thrift], defaults to 'text'"
    opt[String]('z', "timezone")             action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"

  }

  type Namespace = String

  def cmd = IvoryCmd[CliArguments](parser,
      CliArguments("", "", None, DateTimeZone.getDefault, 256.mb, TextFormat),
      IvoryRunner(configuration => c => for {
        repo     <- Repository.fromUriResultTIO(c.repo, configuration)
        inputRef <- Reference.fromUriResultTIO(c.input, configuration)
        factset  <- run(repo, inputRef, c.namespace, c.timezone, c.optimal, c.format)
      } yield List(s"Successfully imported '${c.input}' as ${factset} into '${c.repo}'")))

  def run(repo: Repository, input: ReferenceIO, namespace: Option[Name], timezone: DateTimeZone, optimal: BytesQuantity, format: Format): ResultTIO[FactsetId] =
    fatrepo.ImportWorkflow.onStore(importFeed(input, namespace, optimal, format), timezone).run(IvoryRead.prod(repo))

  def importFeed(input: ReferenceIO, singleNamespace: Option[Name], optimal: BytesQuantity, format: Format)(factset: FactsetId, errorRef: ReferenceIO, timezone: DateTimeZone): IvoryTIO[Unit] = for {
    dict <- dictionaryFromIvoryT
    _    <- IvoryT.fromResultT(repo => for {
      conf <- repo match {
        case r: HdfsRepository => ResultT.ok[IO, Configuration](r.configuration)
        case _                 => ResultT.fail[IO, Configuration]("Currently only support HDFS repository")
      }
      path <- Reference.hdfsPath(input)
      list <- singleNamespace.cata(Namespaces.namespaceSizesSingle(path, _).map(List(_)), Namespaces.namespaceSizes(path)).run(conf)
      _    <- EavtTextImporter.onStore(repo, dict, factset, list.map(_._1), singleNamespace, input, errorRef, timezone, list, optimal, format)
    } yield ())
  } yield ()
}
