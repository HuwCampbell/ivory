package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.control._

import pirate._, Pirate._

object convertDictionary extends IvoryApp {

  case class CliArguments(input: String, output: String)

  val parser = Command(
    "convert-dictionary"
  , Some("Convert a dictionary from the old format to the latest and output the contents to a specified file")
  , CliArguments |*| (
    flag[String](both('i', "input"), description("Dictionary file to convert"))
  , flag[String](both('o', "output"), description("File to output new dictionary to"))
  ))

  val cmd = IvoryCmd.cmd[CliArguments](parser, IvoryRunner { conf => {
    case CliArguments(input, output) =>
      IvoryT.fromRIO { for {
        in         <- IvoryLocation.fromUri(input, conf)
        out        <- IvoryLocation.fromUri(output, conf)
        dictionary <- DictionaryTextStorage.dictionaryFromIvoryLocation(in)
        _          <- IvoryLocation.writeUtf8(out, DictionaryTextStorageV2.delimitedString(dictionary))
      } yield List(s"File successfully written to $output") }
  }})
}
