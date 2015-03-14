package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.cli._
import com.ambiata.ivory.operation.debug._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO

import pirate._, Pirate._

import scalaz._, Scalaz._

object catThrift extends IvoryApp {

  case class CliArguments(entities: List[String], format: String, input: String, output: String)

  val parser = Command(
      "cat-thrift"
    , Some("""
      |Prints the contents of Ivory thrift ingest/extract files
      |""".stripMargin)
    , CliArguments |*| (
      flag[String](both('e', "entity"), description("Filter entities to display. Default is all entities.")).many
    , flag[String](both('o', "output"), empty)
    , flag[String](both('f', "format"), description(s"""
        |The type of thrift format.
        |Supported formats are [${CatThriftFormat.formats.keys.mkString(", ")}].
        |""".stripMargin))
    , argument[String](metavar("INPUT_PATH") |+| description("Glob path to thrift sequence files or parent dir"))
  ))

  val cmd = IvoryCmd.cmd[CliArguments](parser, IvoryRunner { conf => c => IvoryT.fromRIO { for {
    format <- RIO.fromOption[CatThriftFormat](CatThriftFormat.parseFormat(c.format), s"Unknown thrift format '${c.format}'")
    _      <- CatThrift.run(conf.configuration, c.entities, c.input, format, c.output, conf.codec)
  } yield Nil } })
}
