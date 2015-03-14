package com.ambiata.ivory.cli

import com.ambiata.ivory.operation.update._
import pirate._
import scalaz._, Scalaz._

object update extends IvoryApp {

  case class CliArguments()

  val parser = Command(
    "update"
  , Some("""
    |Update to the latest ivory metadata version.
    |""".stripMargin)
  , ValueParse(CliArguments())
  )

  val cmd = IvoryCmd.withRepoBypassVersionCheck[CliArguments](parser, {
    repo => configuration => flags => c =>
      Update.update.toIvoryT(repo).as(Nil)
  })
}
