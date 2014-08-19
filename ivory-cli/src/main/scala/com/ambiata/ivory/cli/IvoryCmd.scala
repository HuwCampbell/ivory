package com.ambiata.ivory.cli

import com.ambiata.ivory.storage.repository.{Codec, RepositoryConfiguration}
import com.ambiata.mundane.control._
import com.ambiata.saws.core.Clients
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser

import scalaz.effect.IO
import scalaz._, Scalaz._

/**
 * Parse command line arguments and run a program with the IvoryRunner
 */
case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: IvoryRunner[A]) {

  def run(args: Array[String]): IO[Option[Unit]] = {
    val repositoryConfiguration = createRepositoryConfiguration(args)
    parseAndRun(repositoryConfiguration.arguments, runner.run(repositoryConfiguration))
  }

  private def createRepositoryConfiguration(args: Array[String]): RepositoryConfiguration =
    RepositoryConfiguration(
      arguments        = parseHadoopArguments(args)._2.toList,
      s3Client         = Clients.s3,
      hdfs             = () => parseHadoopArguments(args)._1,
      scoobi           = () => createScoobiConfiguration(args),
      compressionCodec = () => Codec())

  /**
   * parse args for hadoop arguments and set them on a fresh Configuration object
   * @return (the new configuration, the arguments without and Hadoop or Scoobi options)
   */
  private def parseHadoopArguments(args: Array[String]): (Configuration, Array[String]) = {
    val configuration = new Configuration
    val parser = new GenericOptionsParser(configuration, removeScoobiArguments(args))
    (configuration, parser.getRemainingArgs)
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
  private def removeScoobiArguments(args: Array[String]): Array[String] = {
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

