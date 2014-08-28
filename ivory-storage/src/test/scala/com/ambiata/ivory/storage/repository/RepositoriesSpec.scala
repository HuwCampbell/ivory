package com.ambiata.ivory.storage.repository

import java.util.UUID

import com.ambiata.ivory.core.Crash
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.mundane.io.{FilePath, Temporary}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.nicta.scoobi.impl.ScoobiConfiguration
import org.specs2.matcher.ThrownExpectations
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._

class RepositoriesSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

Create Repository Tests
-----------------------

Create repository should always create all folders
  Can create repository on HDFS                 $hdfs
  Can create repository on S3                   $s3           ${tag("aws")}
  Can create repository on local file system    $local

"""

  lazy val conf = RepositoryConfiguration(ScoobiConfiguration())

  def hdfs = {
    Temporary.using { tmp =>
      val base = tmp </> "base"
      val repo: Repository = Repository.fromUri(s"hdfs://${base}", conf).toOption.getOrElse(Crash.error(Crash.ResultTIO, "Can not create repository on hdfs"))
      createRepository(repo)
      checkRepository(repo)
    } must beOkLike(_ must contain(true).forall)
  }

  def s3 = {
    val repo: Repository = Repository.fromUri(s"s3://ambiata-dev-view/tests/${UUID.randomUUID()}", conf).toOption.getOrElse(Crash.error(Crash.ResultTIO, "Can not create repository on s3"))
    createRepository(repo)
    checkRepository(repo) must beOkLike(_ must contain(true).forall)
    repo.toStore.deleteAll(Repository.root) must beOk
  }

  def local = {
    Temporary.using { tmp =>
      val base = tmp </> "base"
      val repo: Repository = Repository.fromUri(s"file://${base}", conf).toOption.getOrElse(Crash.error(Crash.ResultTIO, "Can not create repository on local file system"))
      createRepository(repo)
      checkRepository(repo)
    } must beOkLike(_ must contain(true).forall)
  }

  def createRepository(repo: Repository) = {
    Repositories.create(repo) must beOk
    Repositories.create(repo) must beOk
  }

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
