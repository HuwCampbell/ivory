package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.core.FactsetId
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._

import pirate._, Pirate._

object statsFactset extends IvoryApp {
  case class CliArguments(
    factSet: String
  )

  val parser = Command(
    "factset-statistics"
  , Some("""
    | Calculate and store statistics of features in a fact-set
    |""".stripMargin)
  , CliArguments |*|
    flag[String](both('f', "fact-set"), description("Input ivory factset ID."))
  )

  val cmd = IvoryCmd.withRepo[CliArguments](parser, { repo => configuration => flags => c =>

    IvoryT.fromRIO { for {
      hdfs          <- repo.asHdfsRepository
      factsetId     <- RIO.fromOption[FactsetId](FactsetId.parse(c.factSet), s"Could not parse FactsetId ${c.factSet}")
      res            = IvoryRetire.statsFacts(hdfs, factsetId)
      _             <- res.run(configuration.scoobiConfiguration)
    } yield List("🎹 ") }
  })
}
