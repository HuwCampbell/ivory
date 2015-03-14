package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.cli._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.extraction.squash.SquashDumpJob
import com.ambiata.mundane.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

object dumpReduction extends IvoryApp {
  case class CliArguments(entities: List[String], features: List[String], snapshot: String, output: String)

  val parser = Command(
    "debug-dump-reduction", Some("""
    |Dump facts related to the specified features/entities for each stage of the squash as text:
    |  ENTITY|NAMESPACE|ATTRIBUTE|DATETIME|VALUE-IN|VALUE-OUT
    |""".stripMargin), CliArguments |*| (
    flag[String](both('e', "entity"), description("A set of entities to debug")).some
  , flag[String](both('f', "feature"), description("A set of virtual features to debug, or none to include them all")).many
  , flag[String](both('s', "snapshot"), description("The snapshot ID to use as input to the squash"))
  , flag[String](both('o', "output"), description("The output location of the dump"))
  ))

  val cmd = IvoryCmd.withRepo[CliArguments](parser, { repo => conf => c =>
    IvoryT.fromRIO { for {
      sid <- RIO.fromOption[SnapshotId](SnapshotId.parse(c.snapshot), s"Invalid snapshot ${c.snapshot}")
      out <- RIO.fromDisjunctionString[IvoryLocation](IvoryLocation.parseUri(c.output, conf))
      fs  <- RIO.fromDisjunctionString[List[FeatureId]](c.features.traverseU(FeatureId.parse))
      _   <- SquashDumpJob.dump(repo, sid, out, fs, c.entities)
    } yield Nil }
  })
}
