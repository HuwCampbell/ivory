package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.control._
import com.ambiata.mundane.control.RIO
import org.joda.time.DateTimeZone
import scalaz._, effect.IO

object createRepository extends IvoryApp {

  case class CliArguments(path: String, timezone: String)

  val parser = new scopt.OptionParser[CliArguments]("create-repository"){
    head("""
|Create Ivory Repository.
|
|This app will create an empty ivory repository.
|""".stripMargin)

    help("help") text "shows this usage text"
    arg[String]("PATH") action { (x, c) => c.copy(path = x) } required() text
      s"Ivory repository to create."
    opt[String]('z', "timezone") action { (x, c) => c.copy(timezone = x) } required() text
      s"Timezone for all dates to be stored in Ivory.\nFor examples see http://joda-time.sourceforge.net/timezones.html, (eg. Sydney is 'Australia/Sydney')"
  }

  val cmd = IvoryCmd[CliArguments](parser, CliArguments("", ""), IvoryRunner { configuration => c =>
    println("Created configuration: " + configuration)
    IvoryT.fromRIO(for {
      repo     <- Repository.fromUri(c.path, configuration, IvoryFlags.default)
      timezone <- RIO.fromDisjunction[DateTimeZone](DateTimeZoneUtil.forID(c.timezone).leftMap(\&/.This.apply))
      _        <- Repositories.create(repo, RepositoryConfig(MetadataVersion.V2, timezone))
    } yield Nil)
  })
}
