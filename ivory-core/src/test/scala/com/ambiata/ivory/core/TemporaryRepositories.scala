package com.ambiata.ivory
package core

import com.ambiata.mundane.control._
import com.ambiata.notion.core.TemporaryType
import com.ambiata.notion.core.TemporaryType.{Hdfs, S3, Posix}
import scalaz.effect.IO
import TemporaryLocations._
/**
 * Create temporary repositories and run functions with them
 */
trait TemporaryRepositories {

  def createTemporaryRepository(temporaryType: TemporaryType): TemporaryRepository[Repository] =
    TemporaryRepository(createRepository(temporaryType))

  def createRepository(temporaryType: TemporaryType): Repository =
    temporaryType match {
      case Posix =>
        LocalRepository(createUniqueLocalLocation)
      case S3 =>
        S3Repository(createUniqueS3Location, conf.s3TmpDirectory)
      case Hdfs =>
        HdfsRepository(createUniqueHdfsLocation)
    }

  def withRepository[A](temporaryType: TemporaryType)(f: Repository => ResultTIO[A]): ResultTIO[A] = {
    runWithRepository(createRepository(temporaryType))(f)
  }

  def withHdfsRepository[A](f: HdfsRepository => ResultTIO[A]): ResultTIO[A] = {
    runWithRepository(HdfsRepository(createUniqueHdfsLocation))(f)
  }

  /**
   * run a function with a temporary repository that is going to run some setup operations first and
   * finally run a cleanup
   */
  def withTemporaryRepositorySetup[T, R <: Repository](repository: TemporaryRepositorySetup[R])(f: TemporaryRepositorySetup[R] => ResultTIO[T]): ResultTIO[T] =
    for {
      _ <- repository.setup
      t <- ResultT.using(ResultT.safe[IO, TemporaryRepositorySetup[R]](repository))(f)
    } yield t

}

object TemporaryRepositories extends TemporaryRepositories
