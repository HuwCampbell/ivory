package com.ambiata.ivory.cli

import com.ambiata.ivory.operation.update._
import scalaz._, Scalaz._

object update extends IvoryApp {

  case class CliArguments()

  val parser = new scopt.OptionParser[CliArguments]("update") {
    head("""
         |Update to the latest ivory metadata version.
         |""".stripMargin)

    help("help") text "shows this usage text"
  }

  val cmd = IvoryCmd.withRepoBypassVersionCheck[CliArguments](parser, CliArguments(), {
    repo => configuration => c =>
      Update.update.toIvoryT(repo).as(Nil)
  })
}
