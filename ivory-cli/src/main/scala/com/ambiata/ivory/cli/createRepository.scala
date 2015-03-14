package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO

import org.joda.time.DateTimeZone

import pirate._, Pirate._

import scalaz._, Scalaz._

object createRepository extends IvoryApp {

  case class CliArguments(path: String, timezone: String)

  val parser = Command(
    "create-repository"
  , Some("""
      |Create Ivory Repository.
      |
      |This app will create an empty ivory repository.
      |""".stripMargin)
  , CliArguments |*| (
    argument[String](metavar("PATH") |+| description(s"Ivory repository to create."))
  , flag[String](both('z', "timezone"), description(s"""
      |Timezone for all dates to be stored in Ivory.
      |For examples see http://joda-time.sourceforge.net/timezones.html, (eg. Sydney is 'Australia/Sydney')
      |""".stripMargin))
  ))

  val cmd = IvoryCmd.cmd[CliArguments](parser, IvoryRunner { configuration => c =>
    println("Created configuration: " + configuration)
    IvoryT.fromRIO(for {
      repo     <- Repository.fromUri(c.path, configuration)
      timezone <- RIO.fromDisjunction[DateTimeZone](DateTimeZoneUtil.forID(c.timezone).leftMap(\&/.This.apply))
      _        <- Repositories.create(repo, RepositoryConfig(MetadataVersion.latestVersion, timezone))
    } yield Nil)
  })
}
