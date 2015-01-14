package com.ambiata.ivory.cli

import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.repository.Codec
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.cli.ScoptReaders._
import com.ambiata.mundane.control._
import com.ambiata.saws.core.Clients
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.fs.Path

import scalaz.effect.IO
import scalaz._, Scalaz._

/**
 * Parse command line arguments and run a program with the IvoryRunner
 */
case class IvoryCmd[A](parser: scopt.OptionParser[A], initial: A, runner: IvoryRunner[A]) {
  def run(args: Array[String]): RIO[Option[Unit]] = {
    val repositoryConfiguration = createIvoryConfiguration(args)
    parseAndRun(repositoryConfiguration.arguments, runner.run(repositoryConfiguration))
  }

  private def createIvoryConfiguration(args: Array[String]): IvoryConfiguration =
    IvoryConfiguration(
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

  private def parseAndRun(args: Seq[String], result: A => IvoryT[RIO, List[String]]): RIO[Option[Unit]] = {
    parser.parse(args, initial).traverseU(a => for {
        r <- IvoryRead.createIO
        x <- result(a).run.run(r)
      } yield x).liftExceptions.onError(e => {
        println(s"Failed! - ${Result.asString(e)}"); sys.exit(1) }).flatMap(l => l.traverse(_.traverse(RIO.putStrLn(_)).void))
  }
}

object IvoryCmd {
  def diagnostic(repository: Repository, flags: IvoryFlags): RIO[Unit] =
    RIO.safe(System.err.println(
      s"""================================================================================
         |
         |Ivory:
         |  Version:             ${IvoryVersion.get.version}
         |  Path:                ${repository.root.show}
         |  Planning Strategy:   ${flags.plan.render}
         |
         |Hadoop:
         |  Version:             ${org.apache.hadoop.util.VersionInfo.getVersion}
         |
         |JVM:
         |  Version:             ${System.getProperty("java.version")}
         |  Maximum Memory:      ${Runtime.getRuntime.maxMemory}
         |
         |================================================================================
         |""".stripMargin))

  def withRepo[A](parser: scopt.OptionParser[A], initial: A,
                  runner: Repository => IvoryConfiguration => IvoryFlags => A => IvoryT[RIO, List[String]]): IvoryCmd[A] = {
    withRepoBypassVersionCheck(parser, initial, repo => config => flags => c =>
      checkVersion.toIvoryT(repo) >> runner(repo)(config)(flags)(c))
  }

  /** Should _only_ be called by upgrade - everything else related to a repository should call [[withRepo]] */
  def withRepoBypassVersionCheck[A](parser: scopt.OptionParser[A], initial: A,
                                    runner: Repository => IvoryConfiguration => IvoryFlags => A => IvoryT[RIO, List[String]]): IvoryCmd[A] = {
    // Oh god this is an ugly/evil hack - the world will be a better place when we upgrade to Pirate
    // Composition, it's a thing scopt, look it up
    var repoArg: Option[String] = None
    var strategy: Option[StrategyFlag] = None
    parser.opt[String]('r', "repository") action { (x, c) => repoArg = Some(x); c} text
      "Path to an ivory repository, defaults to environment variable IVORY_REPOSITORY if set"
    parser.opt[StrategyFlag]("plan-strategy") action { (x, c) => strategy = Some(x); c} optional() text
      "Run with the specified plan strategy, one of: pessimistic - minimal IO, best answer, higher memory; " +
      "conservative - higher IO, best answer, lower memory; optimistic - higher IO, good answer, quicker."
    new IvoryCmd[A](parser, initial, IvoryRunner(config => c =>
      for {
        repoPath        <- IvoryT.fromRIO { RIO.fromOption[String](repoArg.orElse(sys.env.get("IVORY_REPOSITORY")),
          "-r|--repository was missing or environment variable IVORY_REPOSITORY not set") }
        flags           =  strategy.cata(IvoryFlags.apply, IvoryFlags.default)
        repo            <- IvoryT.fromRIO { Repository.fromUri(repoPath, config) }
        _               <- IvoryT.fromRIO { diagnostic(repo, flags) }
        result          <- runner(repo)(config)(flags)(c)
      } yield result
    ))
  }

  def withCluster[A](parser: scopt.OptionParser[A], initial: A,
                     runner: Repository => Cluster  => IvoryConfiguration => IvoryFlags => A => IvoryT[RIO, List[String]]): IvoryCmd[A] = {
    var syncParallelismArg: Option[Int] = None
    var shadowRepoArg: Option[String] = None
    parser.opt[String]("shadow-repository") action { (x, c) => shadowRepoArg = Some(x); c} optional() text
      "Path to a shadow repository, defaults to environment variable SHADOW_REPOSITORY if set"
    parser.opt[Int]("sync-parallelism") action { (x, c) => syncParallelismArg = Some(x); c} optional() text
      "Number of parallel nodes to run operations with, defaults to 20"
    withRepo(parser, initial, repo => config => flags => c =>
      for {
        shadowPath      <- IvoryT.fromRIO[String] { RIO.ok(shadowRepoArg.orElse(sys.env.get("SHADOW_REPOSITORY")).getOrElse(s"/tmp/ivory-shadow-${UUID.randomUUID()}")) }
        syncParallelism <- IvoryT.fromRIO { RIO.ok[Int](syncParallelismArg.getOrElse(20)) }
        cluster         =  Cluster.fromIvoryConfiguration(new Path(shadowPath), config, syncParallelism)
        result          <- runner(repo)(cluster)(config)(flags)(c)
      } yield result
    )
  }

  def checkVersion: RepositoryTIO[Unit] = for {
    c <- Metadata.configuration
    _ <- RepositoryT.fromRIO { _ => c.metadata match {
      case MetadataVersion.Unknown(x) =>
        RIO.failIO[Unit](s"""The version of ivory you are running [${IvoryVersion.get.version}],
                                |does not know about, or understand the version of the specified
                                |repository [${x}]. Perhaps someone has run `ivory update` on the
                                |repository.""".stripMargin)
      case MetadataVersion.V0 | MetadataVersion.V1 | MetadataVersion.V2 =>
        RIO.failIO[Unit](s"""The version of the ivory repository you are trying to access has
                                |meta-data in a form which is too old to be read, you need to run
                                |run `ivory update` in order for this version of ivory to proceed.
                                |
                                |WARNING: If you run `ivory update` older ivory installs will no
                                |longer be able to access the repository.""".stripMargin)
      case MetadataVersion.V3 =>
        RIO.unit
    } }
  } yield ()
}

/**
 * Represents the run of an Ivory program, with all the necessary configuration
 */
case class IvoryRunner[A](run: IvoryConfiguration => A => IvoryT[RIO, List[String]])

trait IvoryApp {
  // It's important this is a val, not a def, to ensure we don't mutate scopt twice accidentally
  val cmd: IvoryCmd[_]
}
