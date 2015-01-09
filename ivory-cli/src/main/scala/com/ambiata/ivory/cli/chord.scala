package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.api.Ivory._
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.core.IvoryLocation
import com.ambiata.ivory.operation.extraction.Chord
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import scalaz.effect.IO

object chord extends IvoryApp {

  case class CliArguments(entities: String, takeSnapshot: Boolean, squash: SquashConfig, formats: ExtractOutput)

  val parser = Extract.options(new scopt.OptionParser[CliArguments]("chord") {
    head("""
           |Extract the latest features from a given ivory repo using a list of entity id and date pairs
           |
           |The output entity ids will be of the form eid:yyyy-MM-dd
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('c', "entities") action { (x, c) => c.copy(entities = x) }  required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
    opt[Unit]("no-snapshot")     action { (x, c) => c.copy(takeSnapshot = false) }     text "Do not take a new snapshot, just any existing."
    opt[Int]("sample-rate") action { (x, c) => c.copy(squash = c.squash.copy(profileSampleRate = x)) } text
      "Every X number of facts will be sampled when calculating virtual results. Defaults to 1,000,000. " +
        "WARNING: Decreasing this number will degrade performance."
  })(c => f => c.copy(formats = f(c.formats)))

  val cmd = IvoryCmd.withCluster[CliArguments](parser, CliArguments("", true, SquashConfig.default, ExtractOutput()), { repo => cluster => conf => flags => c =>
    IvoryT.fromRIO { for {
      ent  <- IvoryLocation.fromUri(c.entities, conf)
      of   <- Extract.parse(conf, c.formats)
      _    <- RIO.when(of.outputs.isEmpty, RIO.fail[Unit]("No output/format specified"))
      // The problem is that we should be outputting the output with a separate date - currently it's hacked into the entity
      _    <- RIO.when(of.outputs.exists(_._1.format.isThrift), RIO.fail[Unit]("Thrift output for chord not currently supported"))
      r    <- RepositoryRead.fromRepository(repo)
      // TODO Should be using Ivory API here, but the generic return type is lost on the monomorphic function
      x    <- Chord.createChordWithSquash(repo, flags, ent, c.takeSnapshot, c.squash, cluster)
      (out, dict) = x
      _    <- Extraction.extract(of, out, dict, cluster).run(r)
    } yield List(s"Successfully extracted chord from '${repo.root.show}'") }
  })
}
