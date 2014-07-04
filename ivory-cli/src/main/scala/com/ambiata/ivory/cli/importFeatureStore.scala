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

object importFeatureStore extends IvoryApp {

  case class CliArguments(repo: String, name: String, path: String)

  val parser = new scopt.OptionParser[CliArguments]("import-feature-store"){
    head("""
|Import a feature store into an ivory repository.
|
|This app will parse the given feature store file and if valid, import it into the given repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo") action { (x, c) => c.copy(repo = x) } required() text
      s"Path to the repository."
    opt[String]('n', "name") action { (x, c) => c.copy(name = x) } required() text s"Name of the feature store in the repository."
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text s"Hdfs path to feature store to import."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", ""), HadoopCmd { configuration => c =>
      val actions =
        FeatureStoreImporter.onHdfs(HdfsRepository(c.path.toFilePath, configuration, ScoobiRun(configuration)), c.name, new Path(c.path)).run(configuration)

      actions.map {
        case _ => List(s"Successfully imported feature store into ${c.repo} under the name ${c.name}.")
      }
    })
}
