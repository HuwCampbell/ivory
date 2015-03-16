package com.ambiata.ivory.cli

import com.ambiata.ivory.core.IvoryConfiguration
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.ivory.storage.repository.Codec
import com.ambiata.mundane.control._
import com.ambiata.poacher.mr.Args
import com.ambiata.saws.core.Clients

import com.nicta.scoobi.Scoobi._

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
    val ivoryConf = createIvoryConfiguration(args.toList)
    val cmd = Command("ivory", None,
      commands.map(c => subcommand(c.cmd.copy(parse = c.cmd.parse <* helper)) <* helperX).foldLeft1(_ ||| _)
      <* helperX
      <* version(BuildInfo.version)
    )
    // End of the universe
    Runners.runOrFail(ivoryConf.arguments, cmd).flatMap(ir =>
      IvoryRead.createIO.flatMap(r => ir.run(ivoryConf).run(r)).unsafeIO.map({
        case Ok(l) =>
          l.foreach(println)
        case Error(e) =>
          Console.err.println(Result.asString(e))
          sys.exit(1)
      })).unsafePerformIO
  }

  def createIvoryConfiguration(args: List[String]): IvoryConfiguration = {
    val configuration = Args.configuration(removeScoobiArguments(args))
    IvoryConfiguration(
      arguments        = configuration._2,
      s3Client         = Clients.s3,
      hdfs             = () => configuration._1,
      scoobi           = () => createScoobiConfiguration(args),
      compressionCodec = () => Codec())
  }

  /** ugly, but... */
  def createScoobiConfiguration(args: List[String]): ScoobiConfiguration = {
    var sc: ScoobiConfiguration = null
    new ScoobiApp {
      def run = sc = configuration
    }.main(args.toArray)
    sc
  }

  /** remove scoobi arguments if they are passed as: user1 user2 scoobi verbose.all.cluster user3 user4 */
  def removeScoobiArguments(args: List[String]): List[String] = {
    val (before, after) = args.span(_.toLowerCase != "scoobi")
    before ++ after.drop(2)
  }
}
