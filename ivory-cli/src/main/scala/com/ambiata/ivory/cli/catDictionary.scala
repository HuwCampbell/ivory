package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

object catDictionary extends IvoryApp {

  val cmd = Command(
    "cat-dictionary"
  , Some("""
    |Print dictionary as text to standard out, delimited by '|'.
    |""".stripMargin)

  , ( flag[String](both('n', "name"), description("For displaying the contents of an older dictionary")).option
  |@| IvoryCmd.repository

    )((nameOpt, loadRepo) => IvoryRunner(conf => loadRepo(conf).flatMap(repo => {

      val store = DictionaryThriftStorage(repo)
      IvoryT.fromRIO { for {
        dictionary <- nameOpt.flatMap(Identifier.parse) match {
          case Some(iid) => store.loadFromId(DictionaryId(iid)).flatMap(RIO.fromOption(_, s"Dictionary '$iid' could not be found"))
          case None      => store.load
        }
      } yield List(
        DictionaryTextStorageV2.delimitedString(dictionary)
      ) }
  }))))
}
