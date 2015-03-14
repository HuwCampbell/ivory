package com.ambiata.ivory.cli.debug

import com.ambiata.ivory.cli._
import com.ambiata.ivory.operation.debug._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

object catThrift extends IvoryApp {

  val cmd = Command(
      "cat-thrift"
    , Some("""
      |Prints the contents of Ivory thrift ingest/extract files
      |""".stripMargin)

    , ( flag[String](both('e', "entity"), description("Filter entities to display. Default is all entities.")).many
    |@| flag[String](both('f', "format"), description(s"""
        |The type of thrift format.
        |Supported formats are [${CatThriftFormat.formats.keys.mkString(", ")}].
        |""".stripMargin))
    |@| flag[String](both('o', "output"), empty)
    |@| argument[String](metavar("INPUT_PATH") |+| description("Glob path to thrift sequence files or parent dir"))

      )((entities, format, output, input) => IvoryRunner(conf => IvoryT.fromRIO(for {

    format <- RIO.fromOption[CatThriftFormat](CatThriftFormat.parseFormat(format), s"Unknown thrift format '${format}'")
    _      <- CatThrift.run(conf.configuration, entities, input, format, output, conf.codec)
    } yield Nil
  ))))
}
