package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.ivory.operation.ingestion._, DictionaryImporter._
import scalaz._, effect._

object importDictionary extends IvoryApp {

  case class CliArguments(path: String, update: Boolean, force: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("import-dictionary"){
    head("""
|Import dictionary into ivory.
|
|This app will parse the given dictionary and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"

    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to either a single dictionary file or directory of files to import."
    opt[Unit]('u', "update") action { (x, c) => c.copy(update = true) } optional() text s"Update the existing dictionary with extra values."
    opt[Unit]('f', "force")  action { (x, c) => c.copy(force = true) } optional() text s"Ignore any import warnings."
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments("", update = false, force = false), { repository => configuration => c =>
      for {
        source <- Reference.fromUriResultTIO(c.path, configuration)
        opts     = ImportOpts(if (c.update) Update else Override, c.force)
        result  <- DictionaryImporter.fromPath(repository, source, opts)
        _       <- result._1 match {
          case Success(_)        => ResultT.unit[IO]
            // Always print validation errors regardless of force
          case f @ Failure(errors) => ResultT.safe[IO, Unit](errors.list.foreach(println))
        }
        dictId  <- ResultT.fromOption[IO, DictionaryId](result._2, "Invalid dictionary")
      } yield List(s"Successfully imported dictionary ${c.path} into ${repository.root.path} as ${dictId.render}.")
  })
}
