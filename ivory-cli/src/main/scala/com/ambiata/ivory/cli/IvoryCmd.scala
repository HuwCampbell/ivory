package com.ambiata.ivory.cli

import com.ambiata.ivory.cli.PirateReaders._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.mundane.control._

import java.util.UUID

import org.apache.hadoop.fs.Path

import pirate._, Pirate._

import scalaz._, Scalaz._

/**
 * Parse command line arguments and run a program with the IvoryRunner
 */
case class IvoryCmd[A](command: Command[A], runner: IvoryRunner[A]) {

  def init(ivoryConf: IvoryConfiguration): Command[RIO[Unit]] =
    command.copy(parse = command.parse.map(a =>
      for {
        r <- IvoryRead.createIO
        l <- runner.run(ivoryConf)(a).run.run(r)
        _ <- l.traverse(RIO.putStrLn(_)).void
      } yield ()
    ))
}

object IvoryCmd {

  case class RepositoryArgs(repositoryPath: Option[String], strategy: Option[StrategyFlag])

  case class ClusterArgs(shadowPath: String, syncParallelism: Int)

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

  def cmd[A](command: Command[A], runner: IvoryRunner[A]): IvoryCmd[A] =
    IvoryCmd(command, runner)

  def withRepo[A](command: Command[A],
                  runner: Repository => IvoryConfiguration => IvoryFlags => A => IvoryT[RIO, List[String]]): IvoryCmd[(A, RepositoryArgs)] =
    withRepoBypassVersionCheck(command, repo => config => flags => c =>
      checkVersion.toIvoryT(repo) >> runner(repo)(config)(flags)(c))

  /** Should _only_ be called by upgrade - everything else related to a repository should call [[withRepo]] */
  def withRepoBypassVersionCheck[A](command: Command[A],
                                    runner: Repository => IvoryConfiguration => IvoryFlags => A => IvoryT[RIO, List[String]]): IvoryCmd[(A, RepositoryArgs)] = {

    val commandRepo = command.copy(parse = command.parse tuple (RepositoryArgs |*| (
        flag[String](both('r', "repository"), description(
           "Path to an ivory repository, defaults to environment variable IVORY_REPOSITORY if set"))
        .map(some).default(sys.env.get("IVORY_REPOSITORY"))
      , flag[StrategyFlag](long("plan-strategy"), description(
          "Run with the specified plan strategy, one of: pessimistic - minimal IO, best answer, higher memory; " +
          "conservative - higher IO, best answer, lower memory; optimistic - higher IO, good answer, quicker.")).option
    )))
    new IvoryCmd(commandRepo, IvoryRunner(config => c =>
      for {
        repoPath        <- IvoryT.fromRIO { RIO.fromOption[String](c._2.repositoryPath,
          "-r|--repository was missing or environment variable IVORY_REPOSITORY not set") }
        flags           =  c._2.strategy.cata(IvoryFlags.apply, IvoryFlags.default)
        repo            <- IvoryT.fromRIO { Repository.fromUri(repoPath, config) }
        _               <- IvoryT.fromRIO { diagnostic(repo, flags) }
        result          <- runner(repo)(config)(flags)(c._1)
      } yield result
    ))
  }

  def withCluster[A](command: Command[A],
                     runner: Repository => Cluster  => IvoryConfiguration => IvoryFlags => A => IvoryT[RIO, List[String]]): IvoryCmd[((A, ClusterArgs), RepositoryArgs)] = {

    val commandCluster = command.copy(parse = command.parse tuple (ClusterArgs |*| (
      flag[String](long("shadow-repository"), description(
        "Path to a shadow repository, defaults to environment variable SHADOW_REPOSITORY if set"))
        .default(sys.env.getOrElse("SHADOW_REPOSITORY", s"/tmp/ivory-shadow-${UUID.randomUUID()}"))
    , flag[Int](long("sync-parallelism"), description("Number of parallel nodes to run operations with, defaults to 20"))
        .default(20)
    )))
    withRepo[(A, ClusterArgs)](commandCluster, repo => config => flags => c => {
      val cluster = Cluster.fromIvoryConfiguration(new Path(c._2.shadowPath), config, c._2.syncParallelism)
      runner(repo)(cluster)(config)(flags)(c._1)
    })
  }

  def checkVersion: RepositoryTIO[Unit] = for {
    c <- Metadata.configuration
    _ <- RepositoryT.fromRIO { _ => c.metadata match {
      case MetadataVersion.Unknown(x) =>
        RIO.failIO[Unit](s"""The version of ivory you are running [${IvoryVersion.get.version}],
                                |does not know about, or understand the version of the specified
                                |repository [${x}]. Perhaps someone has run `ivory update` on the
                                |repository.""".stripMargin)
      case MetadataVersion.V0 | MetadataVersion.V1 | MetadataVersion.V2 | MetadataVersion.V3 =>
        RIO.failIO[Unit](s"""The version of the ivory repository you are trying to access has
                                |meta-data in a form which is too old to be read, you need to run
                                |run `ivory update` in order for this version of ivory to proceed.
                                |
                                |WARNING: If you run `ivory update` older ivory installs will no
                                |longer be able to access the repository.""".stripMargin)
      case MetadataVersion.V4 =>
        RIO.unit
    } }
  } yield ()
}

/**
 * Represents the run of an Ivory program, with all the necessary configuration
 */
case class IvoryRunner[A](run: IvoryConfiguration => A => IvoryT[RIO, List[String]])

trait IvoryApp {
  val cmd: IvoryCmd[_]
}
