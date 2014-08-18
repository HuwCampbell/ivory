package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.repository._
import scalaz.syntax.bind._

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

  val cmd = IvoryCmd[CliArguments](parser, CliArguments(), IvoryRunner { configuration => c =>
      println("Created configuration: " + configuration)
      Repository.fromUriResultTIO(c.path, configuration) >>= Repositories.create
        .as(Nil)
  })
}
