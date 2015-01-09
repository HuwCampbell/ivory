package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.core.HdfsRepository
import com.ambiata.ivory.core.FactsetId
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._

object statsFactset extends IvoryApp {
  case class CliArguments(
    factSet: String
  )

  val parser = new scopt.OptionParser[CliArguments]("factset-statistics") {
    head("""
           | Calculate and store statistics of features in a fact-set
           |""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('f', "fact-set")   action { (x, c) => c.copy(factSet = x) }  required() text "Input ivory factset ID."
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(""), { repo => configuration => flags => c =>

    IvoryT.fromRIO { for {
      hdfs          <- repo.asHdfsRepository
      factsetId     <- RIO.fromOption[FactsetId](FactsetId.parse(c.factSet), s"Could not parse FactsetId ${c.factSet}")
      res            = IvoryRetire.statsFacts(hdfs, factsetId)
      _             <- res.run(configuration.scoobiConfiguration)
    } yield List("🎹 ") }
  })
}
