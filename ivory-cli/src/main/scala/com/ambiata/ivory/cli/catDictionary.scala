package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._

object catDictionary extends IvoryApp {

  case class CliArguments(name: Option[String])

  val parser = new scopt.OptionParser[CliArguments]("cat-dictionary") {
    head("""
           |Print dictionary as text to standard out, delimited by '|'.
           |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('n', "name")        action { (x, c) => c.copy(name = Some(x)) }           optional()       text
      s"For displaying the contents of an older dictionary"
  }

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(None), repo => conf => {
    case CliArguments(nameOpt) =>
      val store = DictionaryThriftStorage(repo)
      for {
        dictionary <- nameOpt.flatMap(Identifier.parse) match {
          case Some(iid) => store.loadFromId(DictionaryId(iid)).flatMap(ResultT.fromOption(_, s"Dictionary '$iid' could not be found"))
          case None      => store.load
        }
      } yield List(
        DictionaryTextStorageV2.delimitedString(dictionary)
      )
  })
}
