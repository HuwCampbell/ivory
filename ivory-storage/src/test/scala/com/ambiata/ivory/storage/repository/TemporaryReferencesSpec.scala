package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core.TemporaryReferences.{S3 => _, Hdfs => _, Posix => _, _}
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.{PosixStore, Store}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.{Hdfs, HdfsStore}
import com.ambiata.saws.s3.{S3Path, S3, S3Store}
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.Specification
import org.specs2.matcher.MatchResult

class TemporaryReferencesSpec extends Specification { def is = s2"""

 Temporary should clean up its own resources when using a
 ========================================================
   repository on the local file system          $localRepository
   repository on hdfs                           $hdfsRepository
   repository on s3                             $s3Repository         ${tag("aws")}

   reference on the local file system           $localReference
   reference on hdfs                            $hdfsReference
   reference on s3                              $s3Reference          ${tag("aws")}

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
    withRepository(S3Repository(testBucket, s3TempPath, conf))

  def localRepository =
    withRepository(LocalRepository(createUniquePath))

  def hdfsRepository =
    withRepository(HdfsRepository(createUniquePath, conf))

  def s3Store =
    withStore(S3Store(testBucket, s3TempPath, conf.s3Client, conf.s3TmpDirectory))

  def hdfsStore =
    withStore(HdfsStore(conf.configuration, createUniquePath))

  def localStore =
    withStore(PosixStore(createUniquePath))

  def s3Reference =
    withReferenceFile(Reference(S3Store(testBucket, s3TempPath, conf.s3Client, conf.s3TmpDirectory), FilePath("data")))

  def hdfsReference =
    withReferenceFile(Reference(HdfsStore(conf.configuration, createUniquePath), FilePath("data")))

  def localReference =
    withReferenceFile(Reference(PosixStore(createUniquePath), FilePath("data")))

  def localLocation =
    withLocationFile(LocalLocation(createUniquePath.path))

  def s3Location =
    withLocationFile(S3Location(testBucket, s3TempPath.path))

  def hdfsLocation =
    withLocationFile(HdfsLocation(createUniquePath.path))

  def localDirLocation =
    withLocationDir(LocalLocation(createUniquePath.path))

  def hdfsDirLocation =
    withLocationDir(HdfsLocation(createUniquePath.path))

  def s3DirLocation =
    withLocationDir(S3Location(testBucket, s3TempPath.path))

  def withRepository(repository: Repository): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryReferences.runWithRepository(repository)(repo => for {
        _ <- Repositories.create(repo)
        x <- repo.toStore.exists(Repository.root </> ".allocated")
      } yield x)
      y <- repository.toStore.exists(Repository.root </> ".allocated")
    } yield (x,y)) must beOkValue(true -> false)

  def withStore(store: Store[ResultTIO]): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryReferences.runWithStore(store)(tmpStore => for {
        _   <- tmpStore.utf8.write(Repository.root </> "test", "")
        dir <- tmpStore.exists(Repository.root </> "test")
      } yield dir)
      y <- store.exists(Repository.root </> "test")
    } yield (x,y)) must beOkValue((true,false))

  def withReferenceFile(reference: ReferenceIO): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <-TemporaryReferences.runWithReference(reference)(ref => for {
        _   <- ref.store.utf8.write(ref.path, "")
        dir <- ref.store.exists(ref.path)
      } yield dir)
      y <- reference.store.exists(reference.path)
    } yield (x, y)) must beOkValue((true,false))

  def withLocationFile(location: Location): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryReferences.runWithLocationFile(location)(loc => for {
        _   <- loc match {
          case LocalLocation(s) => Files.write(FilePath(s), "")
          case S3Location(b, k) => S3.storeObject(S3Path.filePath(b, k), S3Path.filePath(b, k).toFile).executeT(conf.s3Client)
          case HdfsLocation(s)  => Hdfs.writeWith(FilePath(s).toHdfs, out => Streams.write(out, "")).run(conf.configuration)
        }
        dir <- checkFileLocation(loc)
      } yield dir)
      y <- checkFileLocation(location)
    } yield (x, y)) must beOkValue((true,false))


  def checkFileLocation(location: Location): ResultTIO[Boolean] = location match {
    case LocalLocation(s) => Files.exists(FilePath(s))
    case S3Location(b, k) => S3.exists(b, k).executeT(conf.s3Client)
    case HdfsLocation(s)  => Hdfs.exists(FilePath(s).toHdfs).run(conf.configuration)
  }

  def withLocationDir(location: Location): MatchResult[ResultTIO[(Boolean, Boolean)]] =
    (for {
      x <- TemporaryReferences.runWithLocationDir(location)(loc => for {
        _   <- loc match {
          case LocalLocation(s) => Directories.mkdirs(FilePath(s))
          case S3Location(b, k) => S3.storeObject(S3Path.filePath(b, k), S3Path.filePath(b, k).toFile).executeT(conf.s3Client)
          case HdfsLocation(s)  => Hdfs.mkdir(FilePath(s).toHdfs).run(conf.configuration)
        }
        dir <- checkDirLocation(loc)
      } yield dir)
      y <- checkDirLocation(location)
    } yield (x, y)) must beOkValue((true,false))

  def checkDirLocation(location: Location): ResultTIO[Boolean] = location match {
    case LocalLocation(s) => Directories.exists(FilePath(s))
    case S3Location(b, k) => S3.exists(b, k).executeT(conf.s3Client)
    case HdfsLocation(s)  => Hdfs.exists(FilePath(s).toHdfs).run(conf.configuration)
  }
}
