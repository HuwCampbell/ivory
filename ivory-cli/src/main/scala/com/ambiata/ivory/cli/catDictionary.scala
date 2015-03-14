package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.control._

import pirate._, Pirate._

object catDictionary extends IvoryApp {

  case class CliArguments(name: Option[String])

  val parser = Command(
    "cat-dictionary"
  , Some("""
    |Print dictionary as text to standard out, delimited by '|'.
    |""".stripMargin)
  , CliArguments |*|
    flag[String](both('n', "name"), description("For displaying the contents of an older dictionary")).option
  )

  val cmd = IvoryCmd.withRepo[CliArguments](parser, repo => conf => {
    case CliArguments(nameOpt) =>
      val store = DictionaryThriftStorage(repo)
      IvoryT.fromRIO { for {
        dictionary <- nameOpt.flatMap(Identifier.parse) match {
          case Some(iid) => store.loadFromId(DictionaryId(iid)).flatMap(RIO.fromOption(_, s"Dictionary '$iid' could not be found"))
          case None      => store.load
        }
      } yield List(
        DictionaryTextStorageV2.delimitedString(dictionary)
      ) }
  })
}
