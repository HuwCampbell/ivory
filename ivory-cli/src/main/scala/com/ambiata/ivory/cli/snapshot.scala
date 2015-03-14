package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.api.Ivory.SquashConfig
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.cli.PirateReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.api.Ivory.{Date => _, _}
import com.ambiata.ivory.operation.extraction.squash.SquashJob
import com.ambiata.ivory.storage.control._

import java.util.UUID

import org.joda.time.LocalDate

import pirate._, Pirate._

object snapshot extends IvoryApp {

  case class CliArguments(date: Date, squash: SquashConfig, formats: ExtractOutput)

  val parser = Command[CliArguments](
    "snapshot"
  , Some("""
    |Take a snapshot of facts from an ivory repo
    |
    |This will extract the latest facts for every entity relative to a date (default is now)
    |
    |""".stripMargin)
  , CliArguments |*| (
    flag[Date](both('d', "date"), description("Optional date to take snapshot from, default is now."))
      .default(Date.fromLocalDate(LocalDate.now))
  , Extract.parseSquashConfig
  , Extract.parseOutput
  ))

  val cmd = IvoryCmd.withCluster[CliArguments](parser, {
    repo => cluster => configuration => flags => c =>
      val runId = UUID.randomUUID
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${repo.root.show}
                      |  Extract At Date         : ${c.date.slashed}
                      |  Outputs                 : ${c.formats.formats.mkString(", ")}
                      |
                      |""".stripMargin
      println(banner)
      IvoryT.fromRIO { for {
        of        <- Extract.parse(configuration, c.formats)
        snapshot  <- IvoryRetire.takeSnapshot(repo, flags, c.date)
        x         <- SquashJob.squashFromSnapshotWith(repo, snapshot, c.squash, cluster)
        (output, dictionary) = x
        r         <- RepositoryRead.fromRepository(repo)
        _         <- Extraction.extract(of, output, dictionary, cluster).run(r)
      } yield List(banner, s"Snapshot complete: ${snapshot.id}") }
  })
}
