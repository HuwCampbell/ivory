package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.debug._
import com.ambiata.ivory.cli._, ScoptReaders._
import org.apache.hadoop.fs.Path

object dumpFacts extends IvoryApp {
  case class CliArguments(entities: List[String], attributes: List[String], factsets: List[FactsetId], snapshots: List[SnapshotId], output: String)

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
    opt[String]('o', "output") required() action {
      (x, c) => c.copy(output = x)
    }
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(Nil, Nil, Nil, Nil, "not-set"), { repository => conf => c => for {
    output <- IvoryLocation.fromUri(c.output, conf)
    _      <- DumpFacts.dump(repository, DumpFactsRequest(c.factsets, c.snapshots, c.entities, c.attributes), output)
  } yield Nil })
}
