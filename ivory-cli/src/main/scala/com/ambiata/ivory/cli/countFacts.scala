package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.storage.control._

import org.apache.hadoop.fs.Path

import pirate._, Pirate._

import scalaz._, Scalaz._

object countFacts extends IvoryApp {

  val cmd = Command(
    "count-facts"
  , Some("""
    | Count the number of facts in a snapshot
    |""".stripMargin)

  , argument[String](metavar("INPUT_PATH") |+| description("Input path to snapshot"))

    .map(path => IvoryRunner(configuration =>

    IvoryT.fromRIO { IvoryRetire.countFacts(new Path(path, "*")).run(configuration.scoobiConfiguration).map(count => List(count.toString)) }
  )))
}
