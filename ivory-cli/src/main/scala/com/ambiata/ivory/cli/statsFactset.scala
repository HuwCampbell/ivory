package com.ambiata.ivory.cli

import org.apache.hadoop.fs.Path
import com.ambiata.ivory.api.Ivory
import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.core.Repository
import com.ambiata.ivory.core.FactsetId
import com.ambiata.mundane.control._
import com.ambiata.ivory.storage.metadata._

import scalaz.effect._

object statsFactset extends IvoryApp {
  case class CliArguments(
    repo:    String,
    factSet: String
  )

  val parser = new scopt.OptionParser[CliArguments]("factset-statistics") {
    head("""
           | Calculate and store stastics of features in a fact-set
           |""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('r', "repository") action { (x, c) => c.copy(repo = x) }     required() text "Input ivory repository."
    opt[String]('s', "fact-set")   action { (x, c) => c.copy(factSet = x) }  required() text "Input ivory factset ID."
  }

  val cmd = new IvoryCmd[CliArguments](parser, CliArguments("", ""), IvoryRunner { configuration => c =>

    for {
      repo          <- Repository.fromUri(c.repo, configuration)
      store          = DictionaryThriftStorage(repo)
      dictionary    <- store.load
      factset       <- ResultT.fromOption[IO, FactsetId](FactsetId.parse(c.factSet), s"Could not parse FactsetId ${c.factSet}")
      res            = IvoryRetire.statsFacts(dictionary, factset.render)
      _             <- res.run(configuration.scoobiConfiguration)
    } yield (List("🎹 "))
  })
}
