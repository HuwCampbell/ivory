package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.TemporaryLocations._
import com.ambiata.ivory.core.TemporaryIvoryConfiguration._
import com.ambiata.mundane.control.RIO
import com.ambiata.mundane.io._
import com.ambiata.notion.core._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.saws.s3.{S3Prefix, S3, S3Address}
import com.ambiata.saws.s3.TemporaryS3._
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
  def s3Repository =
    withRepository(c => S3Repository(createUniqueS3Location(c), c.s3TmpDirectory))

  def localRepository =
    withRepository(_ => LocalRepository(createUniqueLocalLocation))

  def hdfsRepository =
    withRepository(c => HdfsRepository(createUniqueHdfsLocation(c)))

  def s3Store =
    withStore(c => S3Store(S3Prefix(testBucket, s3TempPath), c.s3Client, c.s3TmpDirectory))

  def hdfsStore =
    withStore(c => HdfsStore(c.configuration, createUniquePath))

  def localStore =
    withStore(_ => PosixStore(createUniquePath))

  def localLocation =
    withLocationFile(_ => createUniqueLocalLocation)

  def s3Location =
    withLocationFile(createUniqueHdfsLocation)

  def hdfsLocation =
    withLocationFile(createUniqueHdfsLocation)

  def localDirLocation =
    withLocationDir(_ => createUniqueLocalLocation)

  def hdfsDirLocation =
    withLocationDir(createUniqueHdfsLocation)

  def s3DirLocation =
    withLocationDir(createUniqueS3Location)

  def withRepository(run: IvoryConfiguration => Repository): MatchResult[RIO[(Boolean, Boolean)]] =
    withConf(c => {
      val repository = run(c)
      (for {
        x <- TemporaryLocations.runWithRepository(repository)(repo => for {
          _ <- Repositories.create(repo, RepositoryConfig.testing)
          x <- repo.store.exists(Repository.root / ".allocated")

        } yield x)
        y <- repository.store.exists(Repository.root / ".allocated")
      } yield (x,y))
    }) must beOkValue(true -> false)

  def withStore(run: IvoryConfiguration => Store[RIO]): MatchResult[RIO[(Boolean, Boolean)]] =
    withConf(c => {
      val s = run(c)
      (for {
        x <- TemporaryStore.runWithStore(s)(tmpStore => for {
          _   <- tmpStore.utf8.write(Repository.root / "test", "")
          dir <- tmpStore.exists(Repository.root / "test")
        } yield dir)
        y <- s.exists(Repository.root / "test")
      } yield (x,y))
    }) must beOkValue((true,false))

  def withLocationFile(run: IvoryConfiguration => IvoryLocation): MatchResult[RIO[(Boolean, Boolean)]] =
    withConf(c => {
      val l = run(c)
      (for {
        x <- TemporaryLocations.runWithIvoryLocationFile(l)(loc => for {
          _   <- loc match {
            case l @ LocalIvoryLocation(LocalLocation(p)) => Files.write(l.filePath, "")
            case s @ S3IvoryLocation(S3Location(b, k), _) => S3Address(b, k).put("").executeT(c.s3Client)
            case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.writeWith(h.toHdfsPath, out => Streams.write(out, "")).run(c.configuration)
          }
          dir <- checkFileLocation(loc, c)
        } yield dir)
        y <- checkFileLocation(l, c)
      } yield (x, y))
    }) must beOkValue((true,false))


  def checkFileLocation(location: IvoryLocation, conf: IvoryConfiguration): RIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(p)) => Files.exists(l.filePath)
    case s @ S3IvoryLocation(S3Location(b, k), _) => S3Address(b, k).exists.executeT(conf.s3Client)
    case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.exists(h.toHdfsPath).run(conf.configuration)
  }

  def withLocationDir(run: IvoryConfiguration => IvoryLocation): MatchResult[RIO[(Boolean, Boolean)]] =
    withConf(c => {
      val l = run(c)
      (for {
        x <- TemporaryLocations.runWithIvoryLocationDir(l)(loc => for {
          _   <- loc match {
            case l @ LocalIvoryLocation(LocalLocation(p)) => Directories.mkdirs(l.dirPath)
            case s @ S3IvoryLocation(S3Location(b, k), _) => S3Address(b, k+"/file").put("").executeT(c.s3Client)
            case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.mkdir(h.toHdfsPath).run(c.configuration)
          }
          dir <- checkDirLocation(loc, c)
        } yield dir)
        y <- checkDirLocation(l, c)
      } yield (x, y))
    }) must beOkValue((true,false))

  def checkDirLocation(location: IvoryLocation, conf: IvoryConfiguration): RIO[Boolean] = location match {
    case l @ LocalIvoryLocation(LocalLocation(p)) => Directories.exists(l.dirPath)
    case S3IvoryLocation(S3Location(b, k), _)     => S3Prefix(b, k).exists.executeT(conf.s3Client)
    case h @ HdfsIvoryLocation(_, _, _, _)        => Hdfs.exists(h.toHdfsPath).run(conf.configuration)
  }
}
