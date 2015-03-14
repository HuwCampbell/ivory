package com.ambiata.ivory.cli.health

import com.ambiata.ivory.cli._, PirateReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

import pirate._, Pirate._

import scalaz._, Scalaz._

object recreate extends IvoryApp {

  val cmd = Command(
    "health-recreate"
  , Some("Recreate factsets using the latest format/compression/block size.")

  , ( flag[FactsetId](both('f', "factset"), empty).some
  |@| IvoryCmd.repository

    )((factsets, loadRepo) => IvoryRunner(conf => loadRepo(conf).flatMap(repo =>

    RecreateFactset.recreateFactsets(repo, factsets).map(re => List(re.successString))
  ))))
}
