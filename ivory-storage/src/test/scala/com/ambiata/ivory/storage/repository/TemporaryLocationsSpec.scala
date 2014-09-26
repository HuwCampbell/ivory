package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryLocations.{S3 => _, Hdfs => _, Posix => _, _}
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.{Hdfs, HdfsStore}
import com.ambiata.saws.s3.{S3Path, S3, S3Store}
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
    withRepository(S3Repository(IvoryLocation.fromDirPath(testBucketDir </> createUniquePath).location, s3TempPath, conf))

  def localRepository =
    withRepository(LocalRepository(createUniqueLocation))

  def hdfsRepository =
    withRepository(HdfsRepository(createUniqueLocation, conf))

  def s3Store =
    withStore(S3Store(testBucket, s3TempPath, conf.s3Client, conf.s3TmpDirectory))

  def hdfsStore =
    withStore(HdfsStore(conf.configuration, createUniquePath))

  def localStore =
    withStore(PosixStore(createUniquePath))

  def localLocation =
    withLocationFile(IvoryLocation(LocalLocation(createUniquePath), conf))

  def s3Location =
    withLocationFile(IvoryLocation(S3Location(testBucketDir </> s3TempPath), conf))

  def hdfsLocation =
    withLocationFile(IvoryLocation(HdfsLocation(createUniquePath), conf))

  def localDirLocation =
    withLocationDir(IvoryLocation(LocalLocation(createUniquePath), conf))

  def hdfsDirLocation =
    withLocationDir(IvoryLocation(HdfsLocation(createUniquePath), conf))

  def s3DirLocation =
    withLocationDir(IvoryLocation(S3Location(testBucketDir </> s3TempPath), conf))

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
        _   <- loc.location match {
          case LocalLocation(s, _) => Files.write(s.toFilePath, "")
          case S3Location(s, _)    => S3.putString(s.toFilePath, "").executeT(conf.s3Client)
          case HdfsLocation(s, _)  => Hdfs.writeWith(loc.toHdfs, out => Streams.write(out, "")).run(conf.configuration)
        }
        dir <- checkFileLocation(loc)
      } yield dir)
      y <- checkFileLocation(location)
    } yield (x, y)) must beOkValue((true,false))


  def checkFileLocation(location: IvoryLocation): ResultTIO[Boolean] = location.location match {
    case LocalLocation(s, _) => Files.exists(s.toFilePath)
    case S3Location(s, _)    => S3.exists(s.toFilePath).executeT(conf.s3Client)
    case HdfsLocation(s, _)  => Hdfs.exists(location.toHdfs).run(conf.configuration)
  }

  def withLocationDir(location: IvoryLocation): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryLocations.runWithIvoryLocationDir(location)(loc => for {
        _   <- loc.location match {
          case LocalLocation(s, _) => Directories.mkdirs(s)
          case S3Location(p, _)    => S3.putString(p.toFilePath, "").executeT(conf.s3Client)
          case HdfsLocation(s, _)  => Hdfs.mkdir(loc.toHdfs).run(conf.configuration)
        }
        dir <- checkDirLocation(loc)
      } yield dir)
      y <- checkDirLocation(location)
    } yield (x, y)) must beOkValue((true,false))

  def checkDirLocation(location: IvoryLocation): ResultTIO[Boolean] = location.location match {
    case LocalLocation(s, _) => Directories.exists(s)
    case S3Location(s, _)    => S3.exists(s.toFilePath).executeT(conf.s3Client)
    case HdfsLocation(s, _R)  => Hdfs.exists(location.toHdfs).run(conf.configuration)
  }
}