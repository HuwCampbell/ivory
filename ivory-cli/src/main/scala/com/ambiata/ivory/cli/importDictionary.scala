package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.ivory.ingest._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.nicta.scoobi.Scoobi._
import scalaz._, effect._

object importDictionary extends IvoryApp {

  case class CliArguments(repo: String, path: String, update: Boolean)

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
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", update = false), HadoopCmd { configuration => c =>
      for {
        repository <- ResultT.fromDisjunction[IO, Repository](Repository.fromUri(c.repo, configuration).leftMap(\&/.This(_)))
        source <- ResultT.fromDisjunction[IO, StorePathIO](StorePath.fromUri(c.path, configuration).leftMap(\&/.This(_)))
        newPathVal <- DictionaryImporter.fromPath(repository, source,
          if (c.update) DictionaryImporter.Update else DictionaryImporter.Override
        )
        newPath <- newPathVal match {
          case Success(path) => ResultT.ok[IO, FilePath](path)
          case Failure(errors) => ResultT.fail[IO, FilePath](errors.list.mkString("\n"))
        }
      } yield List(s"Successfully imported dictionary ${c.path} into ${c.repo} under $newPath.")
  })
}
