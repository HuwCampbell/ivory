package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.cli._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.extraction.squash.SquashDumpJob
import com.ambiata.mundane.control._
import scalaz._, Scalaz._

object dumpReduction extends IvoryApp {
  case class CliArguments(entities: List[String], features: List[String], snapshot: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("debug-dump-reduction") {
    head("""
           |Dump facts related to the specified features/entities for each stage of the squash as text:
           |  ENTITY|NAMESPACE|ATTRIBUTE|DATETIME|VALUE-IN|VALUE-OUT
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('s', "snapshot") action { (x, c) => c.copy(snapshot = x) }               required()             text "The snapshot ID to use as input to the squash"
    opt[String]('o', "output")   action { (x, c) => c.copy(output = x) }                 required()             text "The output location of the dump"
    opt[String]('e', "entity")   action { (x, c) => c.copy(entities = x :: c.entities) } required() unbounded() text "A set of entities to debug"
    opt[String]('f', "feature")  action { (x, c) => c.copy(features = x :: c.features) }            unbounded() text "A set of virtual features to debug, or none to include them all"
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(Nil, Nil, "", ""), { repo => conf => c =>
    IvoryT.fromRIO { for {
      sid <- RIO.fromOption[SnapshotId](SnapshotId.parse(c.snapshot), s"Invalid snapshot ${c.snapshot}")
      out <- RIO.fromDisjunctionString[IvoryLocation](IvoryLocation.parseUri(c.output, conf))
      fs  <- RIO.fromDisjunctionString[List[FeatureId]](c.features.traverseU(FeatureId.parse))
      _   <- SquashDumpJob.dump(repo, sid, out, fs, c.entities)
    } yield Nil }
  })
}
