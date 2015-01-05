package com.ambiata.ivory.cli.health

import com.ambiata.ivory.cli.{IvoryCmd, IvoryRunner, IvoryApp}
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._

import scalaz._, Scalaz._, effect.IO

object recreate extends IvoryApp {
  case class CliArguments(repository: String, factsets: List[String], reducerSize: Option[Long])

  val parser = new scopt.OptionParser[CliArguments]("health-recreate") {
    head("""Recreate factsets using the latest format/compression/block size.""")

    help("help") text "shows this usage text"

    opt[String]('r', "repository") action { (x, c) => c.copy(repository = x) }             required() text "Ivory repository."
    opt[String]('f', "factset")    action { (x, c) => c.copy(factsets = x :: c.factsets) } required() text "Id of factset to recreate."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments(repository = "", factsets = Nil, reducerSize = None),
    IvoryRunner(configuration => c => for {
      repo      <- IvoryT.fromResultTIO(HdfsRepository.fromUri(c.repository, configuration))
      fids      <- c.factsets.traverseU(f => IvoryT.fromResultTIO(ResultT.fromOption(FactsetId.parse(f), s"Invalid factset id '${f}'!")))
      recreated <- RecreateFactset.recreateFactsets(repo, fids)
    } yield List(recreated.successString)))
}
