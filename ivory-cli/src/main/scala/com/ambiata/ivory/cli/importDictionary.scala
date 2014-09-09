package com.ambiata.ivory.cli

import com.ambiata.ivory.core.{Reference, Repository}
import com.ambiata.mundane.control._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.operation.ingestion._, DictionaryImporter._
import scalaz._, effect._

object importDictionary extends IvoryApp {

  case class CliArguments(repo: String, path: String, update: Boolean, force: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("import-dictionary"){
    head("""
|Import dictionary into ivory.
|
|This app will parse the given dictionary and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text
      s"Path to the repository."

    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to either a single dictionary file or directory of files to import."
    opt[Unit]('u', "update") action { (x, c) => c.copy(update = true) } optional() text s"Update the existing dictionary with extra values."
    opt[Unit]('f', "force")  action { (x, c) => c.copy(force = true) } optional() text s"Ignore any import warnings."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", update = false, force = false), IvoryRunner { configuration => c =>
      for {
        repository <- ResultT.fromDisjunction[IO, Repository](Repository.fromUri(c.repo, configuration).leftMap(\&/.This(_)))
        source <- Reference.fromUriResultTIO(c.path, configuration)
        opts     = ImportOpts(if (c.update) Update else Override, c.force)
        result  <- DictionaryImporter.fromPath(repository, source, opts)
        _       <- result._1 match {
          case Success(_)        => ResultT.unit[IO]
            // Always print validation errors regardless of force
          case f @ Failure(errors) => ResultT.safe[IO, Unit](errors.list.foreach(println))
        }
        newPath <- ResultT.fromOption[IO, Identifier](result._2.map(_.id), "Invalid dictionary")
      } yield List(s"Successfully imported dictionary ${c.path} into ${c.repo} under $newPath.")
  })
}
