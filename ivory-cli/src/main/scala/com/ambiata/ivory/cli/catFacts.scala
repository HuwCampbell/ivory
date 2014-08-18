package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.fact.FactsetVersion
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.io._

import com.ambiata.ivory.api.Ivory.printFacts

object catFacts extends IvoryApp {
  case class CliArguments(delimiter: String = "|", tombstone: String = "NA", paths: List[String] = Nil, version: FactsetVersion = FactsetVersion.latest)

  val parser = new scopt.OptionParser[CliArguments]("cat-facts") {
    head("""
           |Print facts as text (ENTITY-NAMESPACE-ATTRIBUTE-VALUE-DATETIME) to standard out, delimited by '|' or explicitly set delimiter.
           |The tombstone value is 'NA' by default
           |The file version is expected to be the latest by default
           |""".stripMargin)

    help("help") text "shows this usage text"
    arg[String]("INPUT_PATH")       action { (x, c) => c.copy(paths = x :: c.paths) } required() unbounded() text
      "Glob path to snapshot facts sequence files or parent dir"
    opt[String]('d', "delimiter")   action { (x, c) => c.copy(delimiter = x) }        optional()             text
      "Delimiter (`|` by default)"
    opt[String]('t', "tombstone")   action { (x, c) => c.copy(tombstone = x) }        optional()             text
      "Tombstone (NA by default)"
    opt[String]('v', "version")   action { (x, c) => c.copy(version = FactsetVersion.fromStringOrLatest(x)) }            optional()             text
      "Version (latest by default)"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(), IvoryRunner { conf => c =>
    printFacts(c.paths.map(new Path(_)), conf.configuration, c.delimiter, c.tombstone, c.version).executeT(consoleLogging).map(_ => Nil)
  })
}
