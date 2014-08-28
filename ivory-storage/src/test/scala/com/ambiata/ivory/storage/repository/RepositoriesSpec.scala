package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.storage.{Temporary => T}
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.{ScalaCheck, Specification}

import scalaz.Scalaz._

class RepositoriesSpec extends Specification with ScalaCheck { def is = s2"""

Create Repository Tests
-----------------------

Create repository should always create all folders
  Can create repository on HDFS                 $hdfs
  Can create repository on S3                   $s3           ${tag("aws")}
  Can create repository on local file system    $local

"""

  lazy val conf = RepositoryConfiguration(ScoobiConfiguration())

  def hdfs =
    exists(T.Hdfs)

  def s3 =
    exists(T.S3)

  def local =
    exists(T.Posix)

  def exists(repository: T.TemporaryType) = {
    T.withRepository(repository) { repo =>
      createRepository(repo) >> checkRepository(repo)
    } must beOkLike(_ must contain(true).forall)
  }

  def createRepository(repo: Repository) =
    Repositories.create(repo) >> Repositories.create(repo)

  def checkRepository(repo: Repository): ResultTIO[List[Boolean]] = {
    List(
      Repository.root,
      Repository.dictionaries,
      Repository.featureStores,
      Repository.factsets,
      Repository.snapshots,
      Repository.errors
    ).traverse(x => repo.toStore.exists(x </> ".allocated"))
  }
}
