package com.ambiata.ivory.cli

import scalaz._, Scalaz._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.ingest._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.repository._

import com.nicta.scoobi.Scoobi._

object importDictionary extends IvoryApp {

  case class CliArguments(repo: String, path: String, name: String)

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
    opt[String]('n', "name") action { (x, c) => c.copy(name = x) } required() text s"Name of the dictionary in the repository."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", ""), HadoopCmd { configuration => c =>
      val actions =
        DictionaryImporter.onHdfs(new Path(c.repo), new Path(c.path), c.name).run(configuration)

      actions.map {
        case _ => List(s"Successfully imported dictionary ${c.path} into ${c.repo} under the name ${c.name}.")
      }
  })
}
