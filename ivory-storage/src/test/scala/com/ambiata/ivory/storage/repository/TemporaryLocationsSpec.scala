package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryLocations.{S3 => _, Hdfs => _, Posix => _, _}
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.{Hdfs, HdfsStore}
import com.ambiata.saws.s3.{S3, S3Store}
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.Specification
import org.specs2.matcher.MatchResult

class TemporaryLocationsSpec extends Specification { def is = s2"""

 Temporary should clean up its own resources when using a
 ========================================================
   repository on the local file system          $localRepository
   repository on hdfs                           $hdfsRepository
   repository on s3                             $s3Repository         ${tag("aws")}

   store on the local file system               $localStore
   store on hdfs                                $hdfsStore
   store on s3                                  $s3Store              ${tag("aws")}

   location file on the local file system       $localLocation
   location file on hdfs                        $hdfsLocation
   location file on s3                          $s3Location           ${tag("aws")}

   location directory on the local file system  $localDirLocation
   location directory on hdfs                   $hdfsDirLocation
   location directory on s3                     $s3DirLocation        ${tag("aws")}

"""

  val conf = IvoryConfiguration.fromScoobiConfiguration(ScoobiConfiguration())

  def s3Repository =
    withRepository(S3Repository(createUniqueS3Location, s3TempPathDir))

  def localRepository =
    withRepository(LocalRepository(createUniqueLocalLocation))

  def hdfsRepository =
    withRepository(HdfsRepository(createUniqueHdfsLocation))

  def s3Store =
    withStore(S3Store(testBucket, s3TempPathDir, conf.s3Client, conf.s3TmpDirectory))

  def hdfsStore =
    withStore(HdfsStore(conf.configuration, createUniquePath))

  def localStore =
    withStore(PosixStore(createUniquePath))

  def localLocation =
    withLocationFile(createUniqueLocalLocation)

  def s3Location =
    withLocationFile(createUniqueHdfsLocation)

  def hdfsLocation =
    withLocationFile(createUniqueHdfsLocation)

  def localDirLocation =
    withLocationDir(createUniqueLocalLocation)

  def hdfsDirLocation =
    withLocationDir(createUniqueHdfsLocation)

  def s3DirLocation =
    withLocationDir(createUniqueS3Location)

  def withRepository(repository: Repository): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryLocations.runWithRepository(repository)(repo => for {
        _ <- Repositories.create(repo)
        x <- repo.store.exists(Repository.root / ".allocated")
      } yield x)
      y <- repository.store.exists(Repository.root / ".allocated")
    } yield (x,y)) must beOkValue(true -> false)

  def withStore(store: Store[ResultTIO]): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryLocations.runWithStore(store)(tmpStore => for {
        _   <- tmpStore.utf8.write(Repository.root / "test", "")
        dir <- tmpStore.exists(Repository.root / "test")
      } yield dir)
      y <- store.exists(Repository.root / "test")
    } yield (x,y)) must beOkValue((true,false))

  def withLocationFile(location: IvoryLocation): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryLocations.runWithIvoryLocationFile(location)(loc => for {
        _   <- loc match {
          case l @ LocalIvoryLocation(LocalLocation(p)) => Files.write(l.filePath, "")
          case s @ S3IvoryLocation(S3Location(b, k), _) => S3.putString(b, k, "").executeT(conf.s3Client)
          case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.writeWith(h.toHdfsPath, out => Streams.write(out, "")).run(conf.configuration)
        }
        dir <- checkFileLocation(loc)
      } yield dir)
      y <- checkFileLocation(location)
    } yield (x, y)) must beOkValue((true,false))


  def checkFileLocation(location: IvoryLocation): ResultTIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(p)) => Files.exists(l.filePath)
    case s @ S3IvoryLocation(S3Location(b, k), _) => S3.exists(b, k).executeT(conf.s3Client)
    case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.exists(h.toHdfsPath).run(conf.configuration)
  }

  def withLocationDir(location: IvoryLocation): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryLocations.runWithIvoryLocationDir(location)(loc => for {
        _   <- loc match {
          case l @ LocalIvoryLocation(LocalLocation(p)) => Directories.mkdirs(l.dirPath)
          case s @ S3IvoryLocation(S3Location(b, k), _) => S3.putString(b, k+"/file", "").executeT(conf.s3Client)
          case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.mkdir(h.toHdfsPath).run(conf.configuration)
        }
        dir <- checkDirLocation(loc)
      } yield dir)
      y <- checkDirLocation(location)
    } yield (x, y)) must beOkValue((true,false))

  def checkDirLocation(location: IvoryLocation): ResultTIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(p)) => Directories.exists(l.dirPath)
    case S3IvoryLocation(S3Location(b, k), _)     => S3.existsPrefix(b, k).executeT(conf.s3Client)
    case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.exists(h.toHdfsPath).run(conf.configuration)
  }
}