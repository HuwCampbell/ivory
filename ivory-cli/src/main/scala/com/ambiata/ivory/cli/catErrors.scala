package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._
import com.ambiata.ivory.api.Ivory.printErrors
import scalaz._,Scalaz._

object catErrors extends IvoryApp {
  case class CliArguments(delimiter: String = "|", paths: List[String] = Nil)

  val parser = new scopt.OptionParser[CliArguments]("cat-errors") {
    head("""
           |Print errors as text (LINE-MESSAGE) to standard out, delimited by '|' or explicitly set delimiter.
           |""".stripMargin)

    help("help") text "shows this usage text"
    arg[String]("INPUT_PATH")       action { (x, c) => c.copy(paths = x :: c.paths) } required() unbounded() text
      "Glob path to errors file or parent dir"
    opt[String]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) }        optional()             text
      "Delimiter (`|` by default)"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(), IvoryRunner { conf => c =>
    printErrors(c.paths.map(new Path(_)), conf.configuration, c.delimiter).executeT(consoleLogging).as(Nil)
  })
}
