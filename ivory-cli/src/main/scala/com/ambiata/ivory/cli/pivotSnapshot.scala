package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.api.IvoryRetire

import org.joda.time.LocalDate
import java.util.Calendar

object pivotSnapshot extends IvoryApp {

  case class CliArguments(repo: String, output: String, delim: Char, tombstone: String, date: LocalDate)

  import ScoptReaders.charRead

  val parser = new scopt.OptionParser[CliArguments]("extract-pivot-snapshot") {
    head("""
         |Pivot ivory data using DenseRowTextStorageV1.DenseRowTextStorer
         |
         |This will read partitioned data using PartitionFactThriftStorageV2 and store as row oriented text.
         |A .dictionary file will be stored containing the fields
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")   action { (x, c) => c.copy(repo = x) }       required() text "Path to ivory repository."
    opt[String]('o', "output") action { (x, c) => c.copy(output = x) }     required() text "Path to store pivot data."
    opt[String]("tombstone")   action { (x, c) => c.copy(tombstone = x) }             text "Output value to use for missing data, default is 'NA'"
    opt[Char]("delim")         action { (x, c) => c.copy(delim = x) }                 text "Output delimiter, default is '|'"
    opt[Calendar]("date")      action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."

  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", "", '|', "NA", new LocalDate), IvoryRunner(conf => c => {
      val banner = s"""======================= pivot =======================
                      |
                      |Arguments --
                      |
                      |  Repo Path               : ${c.repo}
                      |  Output Path             : ${c.output}
                      |  Delim                   : ${c.delim}
                      |  Tombstone               : ${c.tombstone}
                      |  Snapshot Date           : ${c.date.toString("yyyy-MM-dd")}
                      |
                      |""".stripMargin
      println(banner)
      for {
        repo   <- Repository.fromUriResultTIO(c.repo, conf)
        output <- Reference.fromUriResultTIO(c.output, conf)
        _      <- IvoryRetire.pivotFromSnapshot(repo, output, c.delim, c.tombstone, Date.fromLocalDate(c.date))
      } yield List(banner, "Status -- SUCCESS")
    }))
}
