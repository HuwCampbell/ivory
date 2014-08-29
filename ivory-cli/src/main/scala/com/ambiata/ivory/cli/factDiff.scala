package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.core._
import scalaz._, Scalaz._

object factDiff extends IvoryApp {

  case class CliArguments(input1: String, input2: String, output: String)

  val parser = new scopt.OptionParser[CliArguments]("fact-diff") {
    head("""
         |Compute diff between two sets of sequence files containing facts
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]("input1") action { (x, c) => c.copy(input1 = x) } required() text s"Hdfs glob path to the first facts."
    opt[String]("input2") action { (x, c) => c.copy(input2 = x) } required() text s"Hdfs glob path to the second facts."
    opt[String]('o', "output") action { (x, c) => c.copy(output = x) } required() text s"Hdfs location to store the difference."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", ""), IvoryRunner { configuration => c =>

    val action =
      for {
        input1 <- Reference.fromUriAsDir(c.input1, configuration)
        input2 <- Reference.fromUriAsDir(c.input2, configuration)
        output <- Reference.fromUriAsDir(c.output, configuration)
        _      <- IvoryRetire.diffFacts(input1, input2, output).run(configuration.scoobiConfiguration)
      } yield ()

    action.as(List(s"Any differences can be found in '${c.output}'"))

  })
}
