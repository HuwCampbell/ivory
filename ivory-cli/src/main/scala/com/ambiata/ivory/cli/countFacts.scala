package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.ivory.tools.FactCount

object countFacts extends IvoryApp {
  case class CliArguments(path: String)

  val parser = new scopt.OptionParser[CliArguments]("count-facts") {
    head("""
           | Count the number of facts in a snapshot
           |""".stripMargin)

    help("help") text "shows this usage text"
    arg[String]("INPUT_PATH") action { (x, c) => c.copy(path = x) } required() text "Input path to snapshot"
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments(""), ScoobiRunner { configuration => c =>
    FactCount.flatFacts(new Path(c.path, "*")).run(configuration).map(count => List(count.toString))
  })
}
