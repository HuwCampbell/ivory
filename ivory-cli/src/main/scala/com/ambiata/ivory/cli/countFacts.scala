package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.storage.control._

import org.apache.hadoop.fs.Path

import pirate._, Pirate._

import scalaz._, Scalaz._

object countFacts extends IvoryApp {
  case class CliArguments(path: String)

  val parser = Command[CliArguments](
    "count-facts"
  , Some("""
    | Count the number of facts in a snapshot
    |""".stripMargin)
  , CliArguments |*|
    argument[String](metavar("INPUT_PATH") |+| description("Input path to snapshot"))
  )

  val cmd = IvoryCmd.cmd[CliArguments](parser, IvoryRunner { configuration => c =>
    IvoryT.fromRIO { IvoryRetire.countFacts(new Path(c.path, "*")).run(configuration.scoobiConfiguration).map(count => List(count.toString)) }
  })
}
