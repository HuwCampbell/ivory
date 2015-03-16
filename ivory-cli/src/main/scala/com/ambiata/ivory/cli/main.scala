package com.ambiata.ivory.cli

import com.ambiata.ivory.core.IvoryConfiguration
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.ivory.storage.repository.Codec
import com.ambiata.mundane.control._
import com.ambiata.saws.core.Clients

import com.nicta.scoobi.impl.ScoobiConfigurationImpl
import org.apache.hadoop.conf.Configuration

import pirate._, Pirate._

import scalaz._, Scalaz._

object main {

  val commands: NonEmptyList[IvoryApp] = NonEmptyList(
    admin.renameFacts,
    catDictionary,
    catErrors,
    chord,
    config,
    convertDictionary,
    countFacts,
    createRepository,
    debug.catThrift,
    debug.dumpFacts,
    debug.dumpReduction,
    health.recreate,
    importDictionary,
    ingest,
    snapshot,
    statsFactset,
    update
  )

  def main(args: Array[String]): Unit = {
    val cmd = Command("ivory", None,
      commands.map(c => subcommand(c.cmd.copy(parse = c.cmd.parse <* helper tuple configuration))).foldLeft1(_ ||| _)
      <* helperX
      <* version(BuildInfo.version)
    )
    // End of the universe
    Runners.runOrFail(args.toList, cmd).flatMap(ir =>
      IvoryRead.createIO.flatMap(r => ir._1.run(ir._2).run(r)).unsafeIO.map({
        case Ok(l) =>
          l.foreach(println)
        case Error(e) =>
          Console.err.println(Result.asString(e))
          sys.exit(1)
      })).unsafePerformIO
  }

  def configuration: Parse[IvoryConfiguration] =
    flag[read.Assign[String, String]](short('D'), hidden)
      .many
      .map(_.map(_.tuple).toMap).map(createIvoryConfiguration)

  def createIvoryConfiguration(args: Map[String, String]): IvoryConfiguration = {
    val configuration = new Configuration
    args.foreach(a => configuration.set(a._1, a._2))
    IvoryConfiguration(
      s3Client         = Clients.s3,
      hdfs             = () => configuration,
      scoobi           = () => new ScoobiConfigurationImpl(configuration),
      compressionCodec = () => Codec())
  }
}
