package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.cli.ScoptReaders._
import com.ambiata.ivory.core.FactsetId
import com.ambiata.ivory.storage.control._

object statsFactset extends IvoryApp {
  case class CliArguments(
    factsetId: FactsetId
  )

  val parser = new scopt.OptionParser[CliArguments]("factset-statistics") {
    head("""
           | Calculate and store statistics of features in a fact-set
           |""".stripMargin)

    help("help") text "shows this usage text"

    opt[FactsetId]('f', "fact-set")   action { (x, c) => c.copy(factsetId = x) }  required() text "Input ivory factset ID."
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(FactsetId.initial), { repo => configuration => flags => c =>
    IvoryT.fromRIO(IvoryRetire.statsFacts(repo, c.factsetId).map(_ => List("🎹 ")))
  })
}
