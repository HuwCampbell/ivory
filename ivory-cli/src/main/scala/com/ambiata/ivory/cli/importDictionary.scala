package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.operation.ingestion._, DictionaryImporter._

import pirate._, Pirate._

import scalaz._, Scalaz._

object importDictionary extends IvoryApp {

  val cmd = Command(
    "import-dictionary"
  , Some("""
    |Import dictionary into ivory.
    |
    |This app will parse the given dictionary and if valid, import it into the given repository.
    |""".stripMargin)

  , ( flag[String](both('p', "path"), description(s"Hdfs path to either a single dictionary file or directory of files to import."))
  |@| switch(both('u', "update"), description("Update the existing dictionary with extra values."))
  |@| switch(both('f', "force"), description("Ignore any import warnings."))
  |@| IvoryCmd.repository

  )((path, update, force, loadRepo) => IvoryRunner(configuration => loadRepo(configuration).flatMap(repository =>

      IvoryT.fromRIO(for {
        source <- IvoryLocation.fromUri(path, configuration)
        opts    = ImportOpts(if (update) Update else Override, force)
        result <- DictionaryImporter.importFromPath(repository, source, opts)
        _      <- result._1 match {
          case Success(_) =>
            RIO.unit
            // Always print validation errors regardless of force
          case f @ Failure(errors) =>
            RIO.safe[Unit](errors.list.foreach(println))

        }
        _       <- RIO.fromOption[DictionaryId](result._2, "Invalid dictionary")
      } yield List(s"Successfully imported dictionary ${path} into ${repository.root.show}"))
  ))))
}
