package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO

import org.joda.time.DateTimeZone

import pirate._, Pirate._

import scalaz._, Scalaz._

object createRepository extends IvoryApp {

  val cmd = Command(
    "create-repository"
  , Some("""
      |Create Ivory Repository.
      |
      |This app will create an empty ivory repository.
      |""".stripMargin)

  , ( argument[String](metavar("PATH") |+| description(s"Ivory repository to create."))
  |@| flag[String](both('z', "timezone"), description(s"""
      |Timezone for all dates to be stored in Ivory.
      |For examples see http://joda-time.sourceforge.net/timezones.html, (eg. Sydney is 'Australia/Sydney')
      |""".stripMargin))

  )((path, timezone) => IvoryRunner(configuration =>

    IvoryT.fromRIO(for {
      _        <- RIO.putStrLn("Created configuration: " + configuration)
      repo     <- Repository.fromUri(path, configuration)
      timezone <- RIO.fromDisjunction[DateTimeZone](DateTimeZoneUtil.forID(timezone).leftMap(\&/.This.apply))
      _        <- Repositories.create(repo, RepositoryConfig(MetadataVersion.latestVersion, timezone))
    } yield Nil)
  )))
}
