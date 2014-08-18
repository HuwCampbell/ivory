package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.api.IvoryRetire._
import org.joda.time.LocalDate
import java.util.Calendar
import java.util.UUID

object snapshot extends IvoryApp {

  case class CliArguments(repo: String, date: LocalDate, incremental: Boolean)

  val parser = new scopt.OptionParser[CliArguments]("extract-snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[String]('r', "repo")    action { (x, c) => c.copy(repo = x) }   required() text "Path to an ivory repository."
    opt[Unit]("no-incremental") action { (x, c) => c.copy(incremental = false) }   text "Flag to turn off incremental mode"
    opt[Calendar]('d', "date")  action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", LocalDate.now(), true), IvoryRunner {
    configuration => c =>
      val runId = UUID.randomUUID
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${c.repo}
                      |  Extract At Date         : ${c.date.toString("yyyy/MM/dd")}
                      |  Incremental             : ${c.incremental}
                      |
                      |""".stripMargin
      println(banner)
      for {
        repo <- Repository.fromUriResultTIO(c.repo, configuration)
        meta <- takeSnapshot(repo, Date.fromLocalDate(c.date), c.incremental)
      } yield List(banner, s"Output path: ${meta.snapshotId}", "Status -- SUCCESS")
  })
}
