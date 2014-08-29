package com.ambiata.ivory.cli

import com.ambiata.ivory.api.Ivory._
import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.mundane.control.ResultT
import scalaz.effect.IO

object chord extends IvoryApp {

  case class CliArguments(entities: String, takeSnapshot: Boolean, formats: ExtractOutput)

  val parser = Extract.options(new scopt.OptionParser[CliArguments]("chord") {
    head("""
           |Extract the latest features from a given ivory repo using a list of entity id and date pairs
           |
           |The output entity ids will be of the form eid:yyyy-MM-dd
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('c', "entities") action { (x, c) => c.copy(entities = x) }  required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
    opt[Unit]("no-snapshot")     action { (x, c) => c.copy(takeSnapshot = false) }     text "Do not take a new snapshot, just any existing."
  })(c => f => c.copy(formats = f(c.formats)))

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments("", true, ExtractOutput()), { repo => conf => c =>
    for {
      repo <- Repository.fromUriResultTIO(c.repo, conf)
      ent  <- Reference.fromUriAsDir(c.entities, conf)
      of   <- Extract.parse(conf, c.formats)
      _    <- ResultT.when(of.outputs.isEmpty, ResultT.fail[IO, Unit]("No output/format specified"))
      out  <- IvoryRetire.chord(repo, ent, c.takeSnapshot)
      _    <- Extraction.extract(of, ChordExtract(repo.toReference(out))).run(IvoryRead.prod(repo))
      // Delete the output file only if successful - could be useful for debugging otherwise
      _    <- repo.toStore.deleteAll(out)
    } yield List(s"Successfully extracted chord from '${repo.root.path}'")
  })
}
