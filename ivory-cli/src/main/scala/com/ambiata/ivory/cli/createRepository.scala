package com.ambiata.ivory.cli

import com.ambiata.ivory.core.Repository
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.control._
import scalaz._, Scalaz._, effect.IO

object createRepository extends IvoryApp {

  case class CliArguments(path: String = "")

  val parser = new scopt.OptionParser[CliArguments]("create-repository"){
    head("""
|Create Ivory Repository.
|
|This app will create an empty ivory repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    arg[String]("PATH") action { (x, c) => c.copy(path = x) } required() text
      s"Ivory repository to create."

  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments(), IvoryRunner { configuration => c =>
      println("Created configuration: " + configuration)
      IvoryT.fromResultTIO { Repository.fromUri(c.path, configuration).>>=(Repositories.create)
        .as(Nil) }
  })
}
