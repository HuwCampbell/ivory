package com.ambiata.ivory.cli

import com.ambiata.mundane.control._
import com.ambiata.ivory.data.Identifier
import com.ambiata.ivory.storage.legacy.{DictionaryTextStorage, DictionaryThriftStorage}
import com.ambiata.ivory.storage.repository._
import com.nicta.scoobi.Scoobi._
import io.mth.pirate._, Pirate._
import scalaz._, Scalaz._, effect._

object catDictionary extends IvoryApp {

  val cmd = new IvoryPirateCmd(
    ^^(
      required[String]('r' -> "repo", "Ivory repository to create. If the path starts with 's3://' we assume that this is a S3 repository"),
      optional[String]('n' -> "name", "For displaying the contents of an older dictionary"),
      optional[Char]('d' -> "delimiter", "Delimiter (`|` by default)")) { (repo, nameOpt, delim) =>
      HadoopCmd(conf =>
        for {
          repo <- ResultT.fromDisjunction[IO, Repository](Repository.fromUri(repo, conf).leftMap(\&/.This(_)))
          store = DictionaryThriftStorage(repo)
          dictionary <- nameOpt.flatMap(Identifier.parse).cata(store.loadFromId, store.load)
        } yield List(
          DictionaryTextStorage.delimitedDictionaryString(dictionary, delim.getOrElse('|'))
        )
        // TODO Fuck you variance
      ): IvoryRunner
    } ~ "cat-dictionary"
      ~~ "Print dictionary as text to standard out, delimited by '|' or explicitly set delimiter."
  )
}
