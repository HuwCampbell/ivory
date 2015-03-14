package com.ambiata.ivory.cli.health

import com.ambiata.ivory.cli._, PirateReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

import pirate._, Pirate._

object recreate extends IvoryApp {
  case class CliArguments(factsets: List[FactsetId])

  val parser = Command(
    "health-recreate"
  ,  Some("Recreate factsets using the latest format/compression/block size.")
  , CliArguments |*|
    flag[FactsetId](both('f', "factset"), empty).some
  )

  val cmd = IvoryCmd.withRepo[CliArguments](parser, { repo => conf => c =>
    RecreateFactset.recreateFactsets(repo, c.factsets).map(re => List(re.successString))
  })
}
