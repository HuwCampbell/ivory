package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.repository.{Codec, RepositoryConfiguration}
import com.ambiata.saws.core.Clients
import com.nicta.scoobi.core._
import com.nicta.scoobi.Scoobi._
import com.ambiata.mundane.control._
import org.apache.hadoop.conf.Configuration
import scalaz._, Scalaz._
import scalaz.effect._

object main {

  val commands: List[IvoryApp] = List(
    catDictionary,
    catErrors,
    catFacts,
    chord,
    convertDictionary,
    countFacts,
    createRepository,
    factDiff,
    generateDictionary,
    generateFacts,
    importDictionary,
    ingest,
    pivot,
    pivotSnapshot,
    recompress,
    recreate,
    snapshot
  )

  def main(args: Array[String]): Unit = {
    val program = for {
      (progName, argsRest) <- args.headOption.map(_ -> args.tail)
      command <- commands.find(_.cmd.parser.programName == progName)
    } yield command.cmd.run(argsRest)
    // End of the universe
    program.sequence.flatMap(_.flatten.fold(usage())(_ => IO.ioUnit)).unsafePerformIO
  }

  def usage(): IO[Unit] = IO {
    val cmdNames = commands.map(_.cmd.parser.programName).mkString("|")
    println(s"Usage: {$cmdNames}")
    sys.exit(1)
  }
}

case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: IvoryRunner[A]) {

  def run(args: Array[String]): IO[Option[Unit]] = {
    val repositoryConfiguration =
      RepositoryConfiguration(
        arguments        = removeScoobiArguments(args).toList,
        s3Client         = Clients.s3,
        hdfs             = () => new Configuration,
        scoobi           = () => createScoobiConfiguration(args),
        compressionCodec = () => Codec())

    parseAndRun(repositoryConfiguration.arguments, runner.run(repositoryConfiguration))
  }

  /** ugly, but... */
  private def createScoobiConfiguration(args: Array[String]) = {
    var sc: ScoobiConfiguration = null
    new ScoobiApp {
      def run = sc = configuration
    }.main(args)
    sc
  }

  /** remove scoobi arguments if they are passed as: user1 user2 scoobi verbose.all.cluster user3 user4 */
  private def removeScoobiArguments(args: Array[String]): Seq[String] = {
    val (before, after) = args.span(_.toLowerCase != "scoobi")
    before ++ after.drop(2)
  }

  private def parseAndRun(args: Seq[String], result: A => ResultTIO[List[String]]): IO[Option[Unit]] = {
    parser.parse(args, initial)
      .traverse(result andThen {
      _.run.map(_.fold(_.foreach(println), e => { println(s"Failed! - ${Result.asString(e)}"); sys.exit(1) }))
    })
  }
}

/**
 * Represents the run of an Ivory program, with all the necessary configuration
 */
case class IvoryRunner[A](run: RepositoryConfiguration => A => ResultTIO[List[String]])

trait IvoryApp {
  def cmd: IvoryCmd[_]
}
