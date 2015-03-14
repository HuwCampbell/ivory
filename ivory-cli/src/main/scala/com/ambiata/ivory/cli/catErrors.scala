package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.ivory.api.Ivory.printErrors
import com.ambiata.ivory.storage.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

object catErrors extends IvoryApp {
  case class CliArguments(delimiter: String, paths: List[String])

  val parser = Command(
    "cat-errors"
  ,  Some("""
     |Print errors as text (LINE-MESSAGE) to standard out, delimited by '|' or explicitly set delimiter.
     |""".stripMargin)
  , CliArguments |*| (
    flag[String](both('d', "delimiter"), description("Delimiter (`|` by default)")).default("|")
  , argument[String](metavar("INPUT_PATH") |+| description("Glob path to errors file or parent dir")).some
  ))

  val cmd = IvoryCmd.cmd[CliArguments](parser, IvoryRunner { conf => c =>
    IvoryT.fromRIO { printErrors(c.paths.map(new Path(_)), conf.configuration, c.delimiter).as(Nil) }
  })
}
