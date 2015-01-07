package com.ambiata.ivory.cli.health

import com.ambiata.ivory.cli._, ScoptReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._

import scalaz._, effect.IO

object recreate extends IvoryApp {
  case class CliArguments(factsets: List[FactsetId])

  val parser = new scopt.OptionParser[CliArguments]("health-recreate") {
    head("""Recreate factsets using the latest format/compression/block size.""")

    help("help") text "shows this usage text"

    opt[FactsetId]('f', "factset") unbounded() optional() action {
      (x, c) => c.copy(factsets = x :: c.factsets)
    }
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(factsets = Nil), { repo => conf => c =>
    RecreateFactset.recreateFactsets(repo, c.factsets).map(re => List(re.successString))
  })
}
