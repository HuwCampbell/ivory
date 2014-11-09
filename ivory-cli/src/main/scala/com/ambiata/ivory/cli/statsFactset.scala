package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.core.HdfsRepository
import com.ambiata.ivory.core.FactsetId
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._

import scalaz.effect._

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

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(""), { repo => configuration => c =>

    IvoryT.fromResultTIO { for {
      repo          <- HdfsRepository.fromUri(repo.root.show, configuration)
      factsetId     <- ResultT.fromOption[IO, FactsetId](FactsetId.parse(c.factSet), s"Could not parse FactsetId ${c.factSet}")
      res            = IvoryRetire.statsFacts(repo, factsetId)
      _             <- res.run(configuration.scoobiConfiguration)
    } yield List("🎹 ") }
  })
}
