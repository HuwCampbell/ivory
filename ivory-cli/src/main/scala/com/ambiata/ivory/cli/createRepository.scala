package com.ambiata.ivory.cli

import scalaz._, Scalaz._, effect._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.parse._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.poacher.hdfs._

import com.nicta.scoobi.Scoobi._

object createRepository extends IvoryApp {

  case class CliArguments(path: String = "")

  val parser = new scopt.OptionParser[CliArguments]("create-repository"){
    head("""
|Create Ivory Repository.
|
|This app will create an empty ivory repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('p', "path") action { (x, c) => c.copy(path = x) } required() text
      s"Ivory repository to create."

  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments(), HadoopRunner { configuration => c =>
      println("Created configuration: " + configuration)

      val actions = for {
        repo <- Repository.fromUriResultTIO(c.path, configuration)
        _    <- CreateRepository.onStore(repo)
      } yield ()

      actions.map {
        case _ => List(s"Repository successfully created under ${c.path}.")
      }
  })
}
