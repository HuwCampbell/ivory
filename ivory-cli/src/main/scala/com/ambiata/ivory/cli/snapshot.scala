package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.core._
import com.ambiata.ivory.api.Ivory.{Date => _, _}
import com.ambiata.ivory.storage.control.IvoryRead
import org.joda.time.LocalDate
import java.util.{Calendar, UUID}

object snapshot extends IvoryApp {

  case class CliArguments(repo: String, date: LocalDate, incremental: Boolean, formats: ExtractOutput)

  val parser = Extract.options(new scopt.OptionParser[CliArguments]("snapshot") {
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
  })(c => f => c.copy(formats = f(c.formats)))

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", LocalDate.now(), true, ExtractOutput()), IvoryRunner {
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
                      |  Outputs                 : ${c.formats.formats.mkString(", ")}
                      |
                      |""".stripMargin
      println(banner)
      for {
        repo <- Repository.fromUriResultTIO(c.repo, configuration)
        of   <- Extract.parse(configuration, c.formats)
        meta <- IvoryRetire.takeSnapshot(repo, Date.fromLocalDate(c.date), c.incremental)
        _    <- Extraction.extract(of, repo.toReference(Repository.snapshot(meta.snapshotId))).run(IvoryRead.prod(repo))
      } yield List(banner, s"Output path: ${meta.snapshotId}", "Status -- SUCCESS")
  })
}
