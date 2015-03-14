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

  def diagnostic(repository: Repository): RIO[Unit] =
    RIO.safe(System.err.println(
      s"""================================================================================
         |
         |Ivory:
         |  Version:             ${IvoryVersion.get.version}
         |  Path:                ${repository.root.show}
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
                  runner: Repository => IvoryConfiguration => A => IvoryT[RIO, List[String]]): IvoryCmd[(A, Option[String])] =
    withRepoBypassVersionCheck(command, repo => config => c =>
      checkVersion.toIvoryT(repo) >> runner(repo)(config)(c))

  /** Should _only_ be called by upgrade - everything else related to a repository should call [[withRepo]] */
  def withRepoBypassVersionCheck[A](command: Command[A],
                                    runner: Repository => IvoryConfiguration => A => IvoryT[RIO, List[String]]): IvoryCmd[(A, Option[String])] = {

    val commandRepo = command.copy(parse = command.parse tuple
        flag[String](both('r', "repository"), description(
           "Path to an ivory repository, defaults to environment variable IVORY_REPOSITORY if set"))
        .map(some).default(sys.env.get("IVORY_REPOSITORY"))
    )
    new IvoryCmd(commandRepo, IvoryRunner(config => c =>
      for {
        repoPath        <- IvoryT.fromRIO { RIO.fromOption[String](c._2,
          "-r|--repository was missing or environment variable IVORY_REPOSITORY not set") }
        repo            <- IvoryT.fromRIO { Repository.fromUri(repoPath, config) }
        _               <- IvoryT.fromRIO { diagnostic(repo) }
        result          <- runner(repo)(config)(c._1)
      } yield result
    ))
  }

  def flags: Parse[RIO[IvoryFlags]] =
    flag[StrategyFlag](long("plan-strategy"), description(
      "Run with the specified plan strategy, one of: pessimistic - minimal IO, best answer, higher memory; " +
        "conservative - higher IO, best answer, lower memory; optimistic - higher IO, good answer, quicker.")).option
      .map(_.cata(IvoryFlags.apply, IvoryFlags.default))
      // Make sure we trace uses of strategy
      .map(flags => RIO.putStrLn(s"Planning Strategy:   ${flags.plan.render}").as(flags))

  def cluster: Parse[IvoryConfiguration => Cluster] =
    (   flag[String](long("shadow-repository"), description(
        "Path to a shadow repository, defaults to environment variable SHADOW_REPOSITORY if set"))
        .default(sys.env.getOrElse("SHADOW_REPOSITORY", s"/tmp/ivory-shadow-${UUID.randomUUID()}"))
    |@| flag[Int](long("sync-parallelism"), description("Number of parallel nodes to run operations with, defaults to 20"))
        .default(20)
    )((shadowPath, syncParallelism) =>
      config => Cluster.fromIvoryConfiguration(new Path(shadowPath), config, syncParallelism)
    )

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
