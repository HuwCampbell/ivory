package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.api.Ivory
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.debug._
import com.ambiata.ivory.cli._, PirateReaders._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO

import pirate._, Pirate._

import scalaz._, Scalaz._

object dumpFacts extends IvoryApp {
  case class CliArguments(entities: List[String], attributes: List[String], factsets: List[FactsetId],
                          snapshots: List[SnapshotId], output: Option[String])

  val parser = Command(
      "debug-dump-facts"
    , Some("""
      |Dump facts related to the specified entity as text:
      |  ENTITY|NAMESPACE|ATTRIBUTE|VALUE|DATETIME|SOURCE
      |""".stripMargin)
    , CliArguments |*| (
      flag[String](both('e', "entity"), description("Default is all entities.")).many
    , flag[String](both('a', "attribute"), description("Default is all attributes.")).many
    , flag[FactsetId](both('f', "factset"), empty).many
    , flag[SnapshotId](both('s', "snapshot"), empty).many
    , flag[String](both('o', "output"), empty).option
  ))

  val cmd = IvoryCmd.withRepo[CliArguments](parser, { repository => conf => c => IvoryT.fromRIO { for {
    output <- c.output.traverse(o => IvoryLocation.fromUri(o, conf))
    request = DumpFactsRequest(c.factsets, c.snapshots, c.entities, c.attributes)
    ret    <- output.cata(out => Ivory.dumpFactsToFile(repository, request, out), Ivory.dumpFactsToStdout(repository, request))
    _      <- RIO.fromDisjunctionString(ret)
  } yield Nil } })
}
