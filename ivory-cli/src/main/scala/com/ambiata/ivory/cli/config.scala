package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.metadata._

object config extends IvoryApp {

  case class CliArguments()

  val parser = new scopt.OptionParser[CliArguments]("config") {
    head("""
           |Manipulate Ivory configuration values.
           |
           |Currently this is limited to printing the configuration only.
           |""".stripMargin)

    help("help") text "shows this usage text"
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(), repo => flags => conf => {
    case CliArguments() =>
      (for {
        config <- Metadata.configuration
      } yield List(RepositoryConfigTextStorage.toJson(config))).toIvoryT(repo)
  })
}
