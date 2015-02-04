package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.api.Ivory
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.debug._
import com.ambiata.ivory.cli._, ScoptReaders._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO
import org.apache.hadoop.fs.Path

import scalaz._, Scalaz._, effect.IO

object dumpFacts extends IvoryApp {
  case class CliArguments(entities: List[String], attributes: List[String], factsets: List[FactsetId], snapshots: List[SnapshotId], output: Option[String])

  val parser = new scopt.OptionParser[CliArguments]("debug-dump-facts") {
    head("""
           |Dump facts related to the specified entity as text:
           |  ENTITY|NAMESPACE|ATTRIBUTE|VALUE|DATETIME|SOURCE
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('e', "entity") unbounded() optional() text "Default is all entities." action {
      (x, c) => c.copy(entities = x :: c.entities)
    }
    opt[String]('a', "attribute") unbounded() optional() text "Default is all attributes." action {
      (x, c) => c.copy(attributes = x :: c.attributes)
    }
    opt[SnapshotId]('s', "snapshot") unbounded() optional() action {
      (x, c) => c.copy(snapshots = x :: c.snapshots)
    }
    opt[FactsetId]('f', "factset") unbounded() optional() action {
      (x, c) => c.copy(factsets = x :: c.factsets)
    }
    opt[String]('o', "output") optional() action {
      (x, c) => c.copy(output = Some(x))
    }
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(Nil, Nil, Nil, Nil, None), { repository => conf => flags => c => IvoryT.fromRIO { for {
    output <- c.output.traverse(o => IvoryLocation.fromUri(o, conf))
    request = DumpFactsRequest(c.factsets, c.snapshots, c.entities, c.attributes)
    ret    <- output.cata(out => Ivory.dumpFactsToFile(repository, request, out), Ivory.dumpFactsToStdout(repository, request))
    _      <- RIO.fromDisjunctionString(ret)
  } yield Nil } })
}
