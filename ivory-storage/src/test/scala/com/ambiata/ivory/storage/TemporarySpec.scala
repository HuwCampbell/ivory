package com.ambiata.ivory.storage

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.TemporaryReferences._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.{PosixStore, Store}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import scalaz.Scalaz._

class TemporarySpec extends Specification { def is = s2"""

 Temporary should clean up its own resources when using a
 ========================================================
   repository on the local file system        $localRepository
   repository on hdfs                         $hdfsRepository
   repository on s3                           $s3Repository         ${tag("aws")}

   reference on the local file system         $localReference
   reference on hdfs                          $hdfsReference
   reference on s3                            $s3Reference          ${tag("aws")}

   store on the local file system             $localStore
   store on hdfs                              $hdfsStore
   store on s3                                $s3Store              ${tag("aws")}
"""

  val conf = RepositoryConfiguration(ScoobiConfiguration())

  def s3Repository =
    withRepository(S3Repository("ambiata-dev-view", s3TempPath, conf))

  def localRepository =
    withRepository(LocalRepository(createLocalTempDirectory))

  def hdfsRepository =
    withRepository(HdfsRepository(createLocalTempDirectory, conf))

  def s3Store =
    withStore(S3Store("ambiata-dev-view", s3TempPath, conf.s3Client, conf.s3TmpDirectory))

  def hdfsStore =
    withStore(HdfsStore(conf.configuration, createLocalTempDirectory))

  def localStore =
    withStore(PosixStore(createLocalTempDirectory))

  def s3Reference =
    withReferenceFile(Reference(S3Store("ambiata-dev-view", s3TempPath, conf.s3Client, conf.s3TmpDirectory), FilePath("data")))

  def hdfsReference =
    withReferenceFile(Reference(HdfsStore(conf.configuration, createLocalTempDirectory), FilePath("data")))

  def localReference =
    withReferenceFile(Reference(PosixStore(createLocalTempDirectory), FilePath("data")))

  def withRepository(repository: Repository): MatchResult[ResultTIO[Boolean]] = {
    TemporaryReferences.runWithRepository(repository)(repo =>
      Repositories.create(repo) >> ReferenceStore.exists(repo.root </> ".allocated")) >>
      ReferenceStore.exists(repository.root </> ".allocated") must beOkValue(false)
  }

  def withStore(store: Store[ResultTIO]) = {
    TemporaryReferences.runWithStore(store)(store =>
      ReferenceStore.writeUtf8(Reference(store) </> "test", "") >> ReferenceStore.exists(Reference(store) </> "test")) >>
    store.exists(DirPath.Root <|> "test") must beOkValue(false)
  }

  def withReferenceFile(reference: ReferenceIO): MatchResult[ResultTIO[Boolean]] = {
    TemporaryReferences.runWithReference(reference)(ref =>
      ReferenceStore.writeUtf8(ref, "") >>
        ReferenceStore.exists(ref)) >>
    ReferenceStore.exists(reference) must beOkValue(false)
  }
}
