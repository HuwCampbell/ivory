package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.mundane.control.ResultTIO
import com.ambiata.notion.core._
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
  Can create repository on local file system    $config

"""

  lazy val conf = IvoryConfiguration.fromScoobiConfiguration(ScoobiConfiguration())

  def hdfs =
    exists(TemporaryType.Hdfs)

  def s3 =
    exists(TemporaryType.S3)

  def local =
    exists(TemporaryType.Posix)

  def exists(repository: TemporaryType) = {
    TemporaryRepositories.withRepository(repository) { repo =>
      createRepository(repo) >> checkRepository(repo)
    } must beOkLike(_ must contain(true).forall)
  }

  def config = prop((config: RepositoryConfig) =>
    TemporaryRepositoriesT.withRepositoryT(TemporaryType.Posix) { for {
        _         <- RepositoryT.fromIvoryTIO(repo => Repositories.createI(repo, config))
        newConfig <- Metadata.configuration
      } yield newConfig
    } must beOkValue(config)
  )

  def createRepository(repo: Repository) =
    Repositories.create(repo, RepositoryConfig.testing) >> Repositories.create(repo, RepositoryConfig.testing)

  def checkRepository(repo: Repository): ResultTIO[List[Boolean]] = {
    List(
      Repository.root,
      Repository.dictionaries,
      Repository.featureStores,
      Repository.factsets,
      Repository.snapshots,
      Repository.errors
    ).traverse(key => repo.store.exists(key / ".allocated"))
  }
}
