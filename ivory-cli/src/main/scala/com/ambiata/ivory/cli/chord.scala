package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory

import scalaz.{DList => _, _}, effect._

object chord extends IvoryApp {

  case class CliArguments(repo: String, output: String, tmp: String, entities: String, takeSnapshot: Boolean, pivot: Boolean, delim: Char, tombstone: String)

  import ScoptReaders.charRead

  val parser = new scopt.OptionParser[CliArguments]("extract-chord") {
    head("""
         |Extract the latest features from a given ivory repo using a list of entity id and date pairs
         |
         |The output entity ids will be of the form eid:yyyy-MM-dd
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")     action { (x, c) => c.copy(repo = x) }      required() text "Path to an ivory repository."
    opt[String]('o', "output")   action { (x, c) => c.copy(output = x) }    required() text "Path to store snapshot."
    opt[String]('t', "tmp")      action { (x, c) => c.copy(tmp = x) }       required() text "Path to store tmp data."
    opt[String]('c', "entities") action { (x, c) => c.copy(entities = x) }  required() text "Path to file containing entity/date pairs (eid|yyyy-MM-dd)."
    opt[Unit]("no-snapshot")     action { (x, c) => c.copy(takeSnapshot = false) }     text "Do not take a new snapshot, just any existing."
    opt[Unit]("pivot")           action { (x, c) => c.copy(pivot = true) }             text "Pivot the output data."
    opt[Char]("delim")           action { (x, c) => c.copy(delim = x) }                text "Delimiter for pivot file, default '|'."
    opt[String]("tombstone")     action { (x, c) => c.copy(tombstone = x) }            text "Tombstone for pivot file, default 'NA'."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", "", true, false, '|', "NA"), IvoryRunner { conf => c =>
    for {
      repo <- Repository.fromUriResultTIO(c.repo, conf)
      out  <- Reference.fromUriResultTIO(c.output, conf)
      tmp  <- Reference.fromUriResultTIO(c.tmp, conf)
      ent  <- Reference.fromUriResultTIO(c.entities, conf)
      _    <- run(repo, out, tmp, ent, c.takeSnapshot, c.pivot, c.delim, c.tombstone)
    } yield List(s"Successfully extracted chord from '${c.repo}' and stored in '${c.output}'")
  })

  def run(repo: Repository, output: ReferenceIO, tmp: ReferenceIO, entities: ReferenceIO, takeSnapshot: Boolean, pivot: Boolean, delim: Char, tombstone: String): ResultTIO[Unit] = {
    val thriftRef = output </> FilePath("thrift")
    val denseRef = output </> FilePath("dense")
    val tmpRef = tmp </> FilePath("chord")
    for {
      _    <- IvoryRetire.chord(repo, entities, thriftRef, tmpRef, takeSnapshot, Codec())
      _    <- if(pivot) {
                println(s"Pivoting extracted chord in '${thriftRef.path}' to '${denseRef.path}'")
                IvoryRetire.pivot(repo, thriftRef, denseRef, delim, tombstone)
              } else ResultT.ok[IO, Unit](())
    } yield ()
  }
}
