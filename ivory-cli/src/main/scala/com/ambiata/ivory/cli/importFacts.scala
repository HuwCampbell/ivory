package com.ambiata.ivory.cli

import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.joda.time.DateTimeZone
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.mundane.io._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.ingest.EavtTextImporter
import com.ambiata.mundane.control.{Error, Ok}
import scalaz._, Scalaz._
import ScoobiS3EMRAction._

object importFacts extends IvoryApp {

  case class CliArguments(repositoryPath: String = "",
                          dictionary: String = "",
                          factset: Factset   = Factset(""),
                          namespace: String  = "",
                          input: String      = "",
                          errors: Option[Path] = None,
                          timezone: DateTimeZone = DateTimeZone.getDefault)

  val parser = new scopt.OptionParser[CliArguments]("import-facts"){
    head("""
           |Facts Importer.
           |
           |Expected format: <entity id>|<feature id>|<value>|<yyyy-MM-dd HH:mm:ss>
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repository").action { (x, c) => c.copy(repositoryPath = x) }.required.
      text (s"Ivory repository to import features into.")

    opt[String]('d', "dictionary")      action { (x, c) => c.copy(dictionary = x) }      required() text s"Dictionary name, used to get encoding of fact."
    opt[String]('f', "factset")         action { (x, c) => c.copy(factset = Factset(x)) }   required() text s"Fact set name to import the feature into."
    opt[String]('n', "namespace")       action { (x, c) => c.copy(namespace = x) } required() text s"Namespace to import features into."
    opt[String]('i', "input")           action { (x, c) => c.copy(input = x)   }   required() text s"path to read EAVT text files from."
    opt[String]('e', "errors")          action { (x, c) => c.copy(errors = Some(new Path(x)))  }  optional() text s"optional path to persist errors in"
    opt[String]('z', "timezone")        action { (x, c) => c.copy(timezone = DateTimeZone.forID(x))   } required() text
      s"timezone for the dates (see http://joda-time.sourceforge.net/timezones.html, for example Sydney is Australia/Sydney)"
  }

  def cmd = IvoryCmd[CliArguments](parser, CliArguments(), ScoobiCmd { configuration => c =>
      val actions: ScoobiAction[Unit] = {
        val repository = HdfsRepository(c.repositoryPath.toFilePath, configuration, ScoobiRun(configuration))
        for {
          dictionary <- ScoobiAction.fromHdfs(InternalDictionaryLoader(repository, c.dictionary).load)
          _          <- EavtTextImporter.onHdfs(repository, dictionary, c.factset, c.namespace, new Path(c.input), c.errors.getOrElse(new Path("errors")), c.timezone, Some(new SnappyCodec))
        } yield ()
      }

      actions.run(configuration).map {
        case _ => List(s"successfully imported into ${c.repositoryPath}")
      }
    })
}
