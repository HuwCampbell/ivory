package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.metadata._

import pirate.{Metadata => _, _}

object config extends IvoryApp {

  val cmd = Command(
    "config"
  , Some("""
    |Manipulate Ivory configuration values.
    |
    |Currently this is limited to printing the configuration only.
    |""".stripMargin)

  , IvoryCmd.repository.map(loadRepo => IvoryRunner(config =>
      for {
        repo <- loadRepo(config)
        config <- Metadata.configuration.toIvoryT(repo)
      } yield List(RepositoryConfigTextStorage.toJson(config))
  )))
}
