package com.ambiata.ivory
package core

import com.ambiata.mundane.control._
import com.ambiata.notion.core.TemporaryType
import com.ambiata.notion.core.TemporaryType.{Hdfs, S3, Posix}
import scalaz.effect.IO
import TemporaryLocations._
import TemporaryIvoryConfiguration._
/**
 * Create temporary repositories and run functions with them
 */
trait TemporaryRepositories {

  def createRepository(temporaryType: TemporaryType, conf: IvoryConfiguration): Repository =
    temporaryType match {
      case Posix =>
        LocalRepository(createUniqueLocalLocation, IvoryFlags.default)
      case S3 =>
        S3Repository(createUniqueS3Location(conf), conf.s3TmpDirectory, IvoryFlags.default)
      case Hdfs =>
        HdfsRepository(createUniqueHdfsLocation(conf), IvoryFlags.default)
    }

  def withRepository[A](temporaryType: TemporaryType)(f: Repository => RIO[A]): RIO[A] =
    withConf(conf =>
      runWithRepository(createRepository(temporaryType, conf))(f))

  def withHdfsRepository[A](f: HdfsRepository => RIO[A]): RIO[A] =
    withConf(conf =>
      runWithRepository(HdfsRepository(createUniqueHdfsLocation(conf), IvoryFlags.default))(f))

  /**
   * run a function with a temporary repository that is going to run some setup operations first and
   * finally run a cleanup
   */
  def withTemporaryRepositorySetup[T, R <: Repository](repository: TemporaryRepositorySetup[R])(f: R => RIO[T]): RIO[T] =
    for {
      _ <- repository.setup
      t <- RIO.using(RIO.safe[TemporaryRepositorySetup[R]](repository))(tmpRepo => f(tmpRepo.repository))
    } yield t

}

object TemporaryRepositories extends TemporaryRepositories
