package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.metadata._

import pirate.{Metadata => _, _}

object config extends IvoryApp {

  case class CliArguments()

  val parser = Command[CliArguments](
    "config"
  , Some("""
    |Manipulate Ivory configuration values.
    |
    |Currently this is limited to printing the configuration only.
    |""".stripMargin)
  , ValueParse(CliArguments())
  )

  val cmd = IvoryCmd.withRepo[CliArguments](parser, repo => flags => conf => {
    case CliArguments() =>
      (for {
        config <- Metadata.configuration
      } yield List(RepositoryConfigTextStorage.toJson(config))).toIvoryT(repo)
  })
}
