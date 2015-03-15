package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.core.FactsetId
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

object statsFactset extends IvoryApp {

  val cmd = Command(
    "factset-statistics"
  , Some("""
    | Calculate and store statistics of features in a fact-set
    |""".stripMargin)
  , (   flag[String](both('f', "fact-set"), description("Input ivory factset ID."))
    |@| IvoryCmd.repository
    )((factSet, r) => IvoryRunner(configuration =>
    r(configuration).flatMap(repo => IvoryT.fromRIO { for {
      hdfs          <- repo.asHdfsRepository
      factsetId     <- RIO.fromOption[FactsetId](FactsetId.parse(factSet), s"Could not parse FactsetId ${factSet}")
      res            = IvoryRetire.statsFacts(hdfs, factsetId)
      _             <- res.run(configuration.scoobiConfiguration)
    } yield List("🎹 ") }
  ))))
}
