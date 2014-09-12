package com.ambiata.ivory.cli

import com.ambiata.ivory.api.Ivory._
import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.core.Repository
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.mundane.io._

object chord extends IvoryApp {

  case class CliArguments(repo: String, output: String, tmp: String, entities: String, takeSnapshot: Boolean,
                          formats: ExtractOutput)

  val parser = Extract.options(new scopt.OptionParser[CliArguments]("extract-chord") {
    head("""
         |Extract the latest features from a given ivory repo using a list of entity id and date pairs
         |
         |The output entity ids will be of the form eid:yyyy-MM-dd
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")     action { (x, c) => c.copy(repo = x) }      required() text "Path to an ivory repository."
    opt[String]('o', "output")   action { (x, c) => c.copy(output = x) }    required() text "Path to store snapshot."
    opt[String]('t', "tmp")      action { (x, c) => c.copy(tmp = x) }       required() text "Path to store tmp data."
    opt[String]('c', "entities") action { (x, c) => c.copy(entities = x) }  required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
    opt[Unit]("no-snapshot")     action { (x, c) => c.copy(takeSnapshot = false) }     text "Do not take a new snapshot, just any existing."
  })(c => f => c.copy(formats = f(c.formats)))

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", "", true, ExtractOutput()), IvoryRunner { conf => c =>
    for {
      repo <- Repository.fromUriResultTIO(c.repo, conf)
      out  <- Reference.fromUriResultTIO(c.output, conf)
      tmp  <- Reference.fromUriResultTIO(c.tmp, conf)
      ent  <- Reference.fromUriResultTIO(c.entities, conf)
      of   <- Extract.parse(conf, c.formats)
      cout  = out </> FilePath("thrift")
      _    <- IvoryRetire.chord(repo, ent, cout, tmp </> FilePath("chord"), c.takeSnapshot)
      _    <- Extraction.extract(of, cout).run(IvoryRead.prod(repo))
    } yield List(s"Successfully extracted chord from '${c.repo}' and stored in '${c.output}'")
  })
}
