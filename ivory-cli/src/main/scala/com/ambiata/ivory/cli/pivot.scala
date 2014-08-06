package com.ambiata.ivory.cli

import com.ambiata.ivory.extract._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.api.IvoryRetire

import org.apache.hadoop.fs.Path
import org.apache.commons.logging.LogFactory

object pivot extends IvoryApp {

  case class CliArguments(input: String, output: String, repo: String, delim: Char, tombstone: String)

  import ScoptReaders.charRead

  val parser = new scopt.OptionParser[CliArguments]("extract-pivot") {
    head("""
         |Pivot ivory data using DenseRowTextStorageV1.DenseRowTextStorer
         |
         |This will read partitioned data using PartitionFactThriftStorageV2 and store as row oriented text.
         |A .dictionary file will be stored containing the fields
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('i', "input")      action { (x, c) => c.copy(input = x) }      required() text "Path to ivory factset."
    opt[String]('o', "output")     action { (x, c) => c.copy(output = x) }     required() text "Path to store pivot data."
    opt[String]('r', "repo")       action { (x, c) => c.copy(repo = x) }       required() text "Path to repository."
    opt[String]("tombstone")       action { (x, c) => c.copy(tombstone = x) }             text "Output value to use for missing data, default is 'NA'"
    opt[Char]("delim")             action { (x, c) => c.copy(delim = x) }                 text "Output delimiter, default is '|'"
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", "", '|', "NA"), ScoobiRunner(conf => c => {
      val banner = s"""======================= pivot =======================
                      |
                      |Arguments --
                      |
                      |  Input Path              : ${c.input}
                      |  Output Path             : ${c.output}
                      |  Repository              : ${c.repo}
                      |  Delim                   : ${c.delim}
                      |  Tombstone               : ${c.tombstone}
                      |
                      |""".stripMargin
      println(banner)
      for {
        repo   <- Repository.fromUriResultTIO(c.repo, conf)
        input  <- Reference.fromUriResultTIO(c.input, conf)
        output <- Reference.fromUriResultTIO(c.output, conf)
        _      <- IvoryRetire.pivot(repo, input, output, c.delim, c.tombstone)
      } yield List(banner, "Status -- SUCCESS")
    }))
}
