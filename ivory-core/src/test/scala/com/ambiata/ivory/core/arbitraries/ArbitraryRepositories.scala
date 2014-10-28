package com.ambiata.ivory.core
package arbitraries

import com.ambiata.notion.core.TemporaryType.{S3, Hdfs, Posix}
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._
import TemporaryRepositories._
import TemporaryLocations._

/**
 * Arbitraries to generate repositories
 */
trait ArbitraryRepositories {

  /**
   * create a temporary repository that will be populated with some metadata and facts
   */
  implicit def TemporaryRepositorySetupArbitrary: Arbitrary[TemporaryRepositorySetup[Repository]] = Arbitrary {
    for {
      repository <- TemporaryRepository.arbitrary
      setup      =  createLocationDir(repository.repo.root)
    } yield TemporaryRepositorySetup(repository, setup)
  }

  implicit def TemporaryRepository: Arbitrary[TemporaryRepository[Repository]] = Arbitrary {
    for {
      repositoryType <- oneOf(Posix, Hdfs, S3)
    } yield createTemporaryRepository(repositoryType)
  }
}

object ArbitraryRepositories extends ArbitraryRepositories
