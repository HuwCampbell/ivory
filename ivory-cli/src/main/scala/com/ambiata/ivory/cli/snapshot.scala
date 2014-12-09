package com.ambiata.ivory.cli

import com.ambiata.ivory.api.IvoryRetire
import com.ambiata.ivory.api.Ivory.SquashConfig
import com.ambiata.ivory.cli.extract._
import com.ambiata.ivory.core._
import com.ambiata.ivory.api.Ivory.{Date => _, _}
import com.ambiata.ivory.operation.extraction.squash.SquashJob
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.metadata.Metadata
import org.joda.time.LocalDate
import java.util.{Calendar, UUID}

import scalaz._, Scalaz._

object snapshot extends IvoryApp {

  case class CliArguments(date: LocalDate, squash: SquashConfig, formats: ExtractOutput, input: Option[String])

  val parser = Extract.options(new scopt.OptionParser[CliArguments]("snapshot") {
    head("""
         |Take a snapshot of facts from an ivory repo
         |
         |This will extract the latest facts for every entity relative to a date (default is now)
         |
         |""".stripMargin)

    help("help") text "shows this usage text"
    opt[Int]("sample-rate") action { (x, c) => c.copy(squash = c.squash.copy(profileSampleRate = x)) } text
      "Every X number of facts will be sampled when calculating virtual results. Defaults to 1,000,000. " +
        "WARNING: Decreasing this number will degrade performance."
    opt[Calendar]('d', "date")  action { (x, c) => c.copy(date = LocalDate.fromCalendarFields(x)) } text
      s"Optional date to take snapshot from, default is now."
    opt[String]('i', "squash-input")  action { (x, c) => c.copy(input = Some(x)) } text
      "HACK: Squash input file."
  })(c => f => c.copy(formats = f(c.formats)))

  val cmd = IvoryCmd.withCluster[CliArguments](parser, CliArguments(LocalDate.now(), SquashConfig.default, ExtractOutput(), None), {
    repo => cluster => configuration => flags => c =>
      val runId = UUID.randomUUID
      val banner = s"""======================= snapshot =======================
                      |
                      |Arguments --
                      |
                      |  Run ID                  : ${runId}
                      |  Ivory Repository        : ${repo.root.show}
                      |  Extract At Date         : ${c.date.toString("yyyy/MM/dd")}
                      |  Outputs                 : ${c.formats.formats.mkString(", ")}
                      |
                      |""".stripMargin
      println(banner)
      IvoryT.fromRIO { for {
        of        <- Extract.parse(configuration, c.formats)
        snapshot  <- IvoryRetire.takeSnapshot(repo, flags, Date.fromLocalDate(c.date))
        r    <- RepositoryRead.fromRepository(repo)
        _    <- c.input.cata(in => for {
          loc  <- IvoryLocation.fromUri(in, configuration)
          hloc <- loc.asHdfsIvoryLocation
          _    = println(s"WARNING: Using squash input file ${in}, which is going to bypass the snapshot")
          d    <- Metadata.latestDictionaryFromIvory(repo)
          _    <- Extraction.extract(of, ShadowOutputDataset.fromIvoryLocation(hloc), d, cluster).run(r)
        } yield (),
          SquashJob.squashFromSnapshotWith(repo, snapshot.toMetadata, c.squash, cluster).flatMap { case (output, dictionary) =>
          Extraction.extract(of, output, dictionary, cluster).run(r)
        })
      } yield List(banner, s"Snapshot complete: ${snapshot.id}") }
  })
}
