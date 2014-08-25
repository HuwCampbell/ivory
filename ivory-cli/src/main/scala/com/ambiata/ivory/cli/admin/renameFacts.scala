package com.ambiata.ivory.cli.admin

import com.ambiata.ivory.cli._
import com.ambiata.ivory.core.{FeatureId, Name}
import com.ambiata.ivory.operation.rename.{Rename, RenameMapping}
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.ivory.storage.repository.{RepositoryConfiguration, Repository}
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.parse.{Delimited, ListParser}, ListParser.string
import scalaz.{Name => _, _}, Scalaz._, effect._

object renameFacts extends IvoryApp {

  case class CliArguments(repo: String, mapping: List[(String, String)], batch: Option[String], reducerSize: Option[Long])

  val parser = new scopt.OptionParser[CliArguments]("rename"){
    head("""
           |Rename a set of fact features (most likely due to legacy).
           |This creates a new factset rather than update the facts in place.
           |
           |WARNING: This operation is likely to take a while, depending on the size of the repository.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")                 action { (x, c) => c.copy(repo = x) }                       required() text
      s"Path to an ivory repository."
    opt[(String, String)]('m', "mapping")    action { (x, c) => c.copy(mapping = x :: c.mapping) }       optional() text
      s"<FROM-NAMESPACE>:<FROM-FEATURE>=<TARGET-NAMESPACE>:<TO-FEATURE>"                                 unbounded()
    opt[String]('b', "batch")                action { (x, c) => c.copy(batch = Some(x)) }                required() text
      s"An optional batch file containing lines of 'from-namespace:from-feature|to-namespace:to-feature'"
    opt[Long]('s', "reducer-size")           action { (x, c) => c.copy(reducerSize = Some(x)) }          optional() text
      "Max size (in bytes) of a reducer used to copy Factsets"
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", List(), None, None), IvoryRunner { conf => c => for {
    repo    <- Repository.fromUriResultTIO(c.repo, conf)
    batch   <- c.batch.cata(parseBatchFile(_, conf), ResultT.ok[IO, RenameMapping](RenameMapping(Nil)))
    mapping <- ResultT.fromDisjunction[IO, RenameMapping](createMapping(c.mapping).leftMap(\&/.This.apply))
    stats   <- Rename.rename(RenameMapping(batch.mapping ++ mapping.mapping), c.reducerSize.map(_.bytes).getOrElse(1.gb)).run(IvoryRead.prod(repo))
  } yield List(s"Successfully renamed ${stats._3.facts} facts to new factset ${stats._1.render}")})

  def createMapping(mapping: List[(String, String)]): String \/ RenameMapping =
    mapping.traverseU { case (f, t) => parseFeatureId(f) tuple parseFeatureId(t) }.map(RenameMapping.apply)

  def parseBatchFile(path: String, conf: RepositoryConfiguration): ResultTIO[RenameMapping] = for {
    ref     <- Reference.fromUriResultTIO(path, conf)
    exists  <- ref.run(_.exists)
    _       <- if (!exists) ResultT.fail[IO, Unit](s"Path ${ref.path} does not exist in ${ref.store}!") else ResultT.unit[IO]
    lines   <- ref.run(_.linesUtf8.read)
    mapping <- ResultT.fromDisjunction[IO, RenameMapping](lines.traverseU(parseLine).map(RenameMapping.apply).leftMap(\&/.This.apply))
  } yield mapping

  def parseLine(line: String): String \/ (FeatureId, FeatureId) = {
    for {
      l <- (string tuple string).run(Delimited.parsePsv(line)).disjunction
      m <- parseFeatureId(l._1) tuple parseFeatureId(l._2)
    } yield m
  }

  def parseFeatureId(featureId: String): String \/ FeatureId =
    (string tuple string).run(Delimited.parseRow(featureId, ':')).disjunction.flatMap {
      case (ns, n) => Name.nameFromStringDisjunction(ns).map(FeatureId(_, n))
    }
}
