package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.api.Ivory.SquashConfig
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.core._
import com.ambiata.ivory.api.Ivory.{Date => _, _}
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.ivory.storage.metadata._
import org.joda.time.LocalDate
import java.util.{Calendar, UUID}

import scalaz._, Scalaz._

object snapshot extends IvoryApp {

  case class CliArguments(date: LocalDate, incremental: Boolean, squash: SquashConfig, formats: ExtractOutput)

  val parser = Extract.options(new scopt.OptionParser[CliArguments]("snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[Unit]("no-incremental") action { (x, c) => c.copy(incremental = false) }   text "Flag to turn off incremental mode"
    opt[Int]("sample-rate") action { (x, c) => c.copy(squash = c.squash.copy(profileSampleRate = x)) } text
      "Every X number of facts will be sampled when calculating virtual results. Defaults to 1,000,000. " +
        "WARNING: Decreasing this number will degrade performance."
    opt[Calendar]('d', "date")  action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
  })(c => f => c.copy(formats = f(c.formats)))

  val cmd = IvoryCmd.withRepo[CliArguments](parser, CliArguments(LocalDate.now(), true, SquashConfig.default, ExtractOutput()), {
    repo => configuration => c =>
      val runId = UUID.randomUUID
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${repo.root.show}
                      |  Extract At Date         : ${c.date.toString("yyyy/MM/dd")}
                      |  Incremental             : ${c.incremental}
                      |  Outputs                 : ${c.formats.formats.mkString(", ")}
                      |
                      |""".stripMargin
      println(banner)
      for {
        of   <- Extract.parse(configuration, c.formats)
        res  <- IvoryRetire.takeSnapshot(repo, Date.fromLocalDate(c.date), c.incremental)
        meta = res.meta
        _    <- Extraction.extract(of, SnapshotExtract(meta, c.squash)).run(IvoryRead.prod(repo))
      } yield List(
        banner,
        s"Output path: ${meta.snapshotId}",
        res.incremental.cata((incr: SnapshotManifest) => s"Incremental snapshot used: ${incr.snapshotId}", "No Incremental Snapshot was used."),
        "Status -- SUCCESS")
  })
}
