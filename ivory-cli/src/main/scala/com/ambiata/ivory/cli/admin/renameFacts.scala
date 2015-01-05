package com.ambiata.ivory.cli.admin

import com.ambiata.ivory.cli._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.rename.{Rename, RenameMapping}
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.parse.ListParser.string
import com.ambiata.mundane.parse.Delimited

import scalaz._, Scalaz._

object renameFacts extends IvoryApp {

  case class CliArguments(mapping: List[(String, String)], batch: Option[String], reducerSize: Option[Long])

  val parser = new scopt.OptionParser[CliArguments]("rename"){
    head("""
           |Rename a set of fact features (most likely due to legacy).
           |This creates a new factset rather than update the facts in place.
           |
           |WARNING: This operation is likely to take a while, depending on the size of the repository.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[(String, String)]('m', "mapping")    action { (x, c) => c.copy(mapping = x :: c.mapping) }       optional() text
      s"<FROM-NAMESPACE>:<FROM-FEATURE>=<TARGET-NAMESPACE>:<TO-FEATURE>"                                 unbounded()
    opt[String]('b', "batch")                action { (x, c) => c.copy(batch = Some(x)) }                required() text
      s"An optional batch file containing lines of 'from-namespace:from-feature|to-namespace:to-feature'"
    opt[Long]('s', "reducer-size")           action { (x, c) => c.copy(reducerSize = Some(x)) }          optional() text
      "Max size (in bytes) of a reducer used to copy Factsets"
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(List(), None, None), { repo => conf => c => IvoryT.fromRIO { for {
    batch   <- c.batch.cata(parseBatchFile(_, conf), RIO.ok[RenameMapping](RenameMapping(Nil)))
    mapping <- RIO.fromDisjunction[RenameMapping](createMapping(c.mapping).leftMap(\&/.This.apply))
    r       <- RepositoryRead.fromRepository(repo)
    stats   <- Rename.rename(RenameMapping(batch.mapping ++ mapping.mapping), c.reducerSize.map(_.bytes).getOrElse(1.gb)).run(r)
  } yield List(s"Successfully renamed ${stats._3.facts} facts to new factset ${stats._1.render}") } })

  def createMapping(mapping: List[(String, String)]): String \/ RenameMapping =
    mapping.traverseU { case (f, t) => FeatureId.parse(f) tuple FeatureId.parse(t) }.map(RenameMapping.apply)

  def parseBatchFile(path: String, conf: IvoryConfiguration): RIO[RenameMapping] = for {
    location <- IvoryLocation.fromUri(path, conf)
    exists   <- IvoryLocation.exists(location)
    _        <- if (!exists) RIO.fail[Unit](s"Path ${location.show} does not exist") else RIO.unit
    lines    <- IvoryLocation.readLines(location)
    mapping  <- RIO.fromDisjunction[RenameMapping](lines.traverseU(parseLine).map(RenameMapping.apply).leftMap(\&/.This.apply))
  } yield mapping

  def parseLine(line: String): String \/ (FeatureId, FeatureId) = {
    for {
      l <- (string tuple string).run(Delimited.parsePsv(line)).disjunction
      m <- FeatureId.parse(l._1) tuple FeatureId.parse(l._2)
    } yield m
  }
}
