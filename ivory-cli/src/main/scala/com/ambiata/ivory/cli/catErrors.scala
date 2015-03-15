package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.ivory.api.Ivory.printErrors
import com.ambiata.ivory.storage.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

object catErrors extends IvoryApp {

  val cmd = Command(
    "cat-errors"
  ,  Some("""
     |Print errors as text (LINE-MESSAGE) to standard out, delimited by '|' or explicitly set delimiter.
     |""".stripMargin)

  , ( flag[String](both('d', "delimiter"), description("Delimiter (`|` by default)")).default("|")
  |@| argument[String](metavar("INPUT_PATH") |+| description("Glob path to errors file or parent dir")).some

    )((delimiter, paths) => IvoryRunner(conf =>

    IvoryT.fromRIO { printErrors(paths.map(new Path(_)), conf.configuration, delimiter).as(Nil) }
  )))
}
