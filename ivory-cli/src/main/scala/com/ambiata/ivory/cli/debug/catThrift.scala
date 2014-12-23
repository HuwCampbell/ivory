package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.cli._
import com.ambiata.ivory.operation.debug._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.ResultT

import scalaz.effect.IO

object catThrift extends IvoryApp {

  case class CliArguments(entities: List[String], format: String, input: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("cat-thrift") {
    head("""
           |Prints the contents of Ivory thrift ingest/extract files
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('e', "entity") unbounded() optional() text "Filter entities to display. Default is all entities." action {
      (x, c) => c.copy(entities = x :: c.entities)
    }
    opt[String]('o', "output") required() action {
      (x, c) => c.copy(output = x)
    }
    opt[String]('f', "format") required() text s"""The type of thrift format. Supported formats are [${CatThriftFormat.formats.keys.mkString(", ")}].""" action {
      (x, c) => c.copy(format = x)
    }
    arg[String]("INPUT_PATH") required() text "Glob path to thrift sequence files or parent dir" action {
      (x, c) => c.copy(input = x)
    }
  }

  val cmd =  new IvoryCmd[CliArguments](parser, CliArguments(Nil, "not-set", "not-set", "not-set"), IvoryRunner { conf => c => IvoryT.fromRIO { for {
    format <- ResultT.fromOption[IO, CatThriftFormat](CatThriftFormat.parseFormat(c.format), s"Unknown thrift format '${c.format}'")
    _      <- CatThrift.run(conf.configuration, c.entities, c.input, format, c.output, conf.codec)
  } yield Nil } })
}
