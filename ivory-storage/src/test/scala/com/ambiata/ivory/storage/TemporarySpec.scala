package com.ambiata.ivory.storage

import com.ambiata.ivory.storage.Temporary._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store.{Reference, ReferenceIO}
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io.FilePath
import com.ambiata.mundane.store.{PosixStore, Store}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.poacher.hdfs.HdfsStore
import com.ambiata.saws.s3.S3Store
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.Specification
import org.specs2.matcher.{MatchResult, ThrownExpectations}

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
    Temporary.runWithRepository(repository)(repo =>
      Repositories.create(repo) >> repo.toStore.exists(Repository.root </> ".allocated")) >>
    repository.toStore.exists(Repository.root </> ".allocated") must beOkValue(false)
  }

  def withStore(store: Store[ResultTIO]) = {
    Temporary.runWithStore(store)(store =>
      store.utf8.write(Repository.root </> "test", "") >> store.exists(Repository.root </> "test")) >>
    store.exists(Repository.root </> "test") must beOkValue(false)
  }

  def withReferenceFile(reference: ReferenceIO): MatchResult[ResultTIO[Boolean]] = {
    Temporary.runWithReference(reference)(ref =>
      ref.store.utf8.write(ref.path, "") >> ref.store.exists(ref.path)) >>
    reference.store.exists(reference.path) must beOkValue(false)
  }
}
