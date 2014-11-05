package com.ambiata.ivory.operation
package arbitraries

import com.ambiata.ivory.core._
import TemporaryLocations._
import TemporaryRepositories._
import arbitraries.ArbitraryFacts._
import com.ambiata.ivory.operation.ingestion._
import DictionaryImporter._
import Ingest._
import com.ambiata.ivory.storage.repository.Repositories
import com.ambiata.mundane.control._
import ResultT._
import com.ambiata.notion.core.TemporaryType
import com.ambiata.notion.core.TemporaryType.{Hdfs, Posix, S3}
import org.scalacheck.Arbitrary
import org.scalacheck.Gen._
import scalaz._, Scalaz._

/**
 * Arbitraries to generate repositories
 *
 * Usage:
 *
 *  import TemporaryRepositories._
 *
 *  prop((repository: TemporaryRepositorySetupArbitrary) => withTemporaryRepositorySetup(repository) { repo: Repository =>
 *    // do something with the repository
 *  })
 *
 */
trait ArbitraryRepositories {

  /**
   * create a temporary repository that will be populated with some metadata and facts
   */
  implicit def TemporaryRepositorySetupArbitrary: Arbitrary[TemporaryRepositorySetup[Repository]] =
    temporaryRepositorySetupArbitrary(List(Posix, Hdfs, S3))

  /** create an arbitrary for specific location types */
  def temporaryRepositorySetupArbitrary(types: List[TemporaryType]): Arbitrary[TemporaryRepositorySetup[Repository]] = Arbitrary {
    for {
      repository          <- temporaryRepository(types).arbitrary
      factsWithDictionary <- FactsWithDictionaryArbitrary.arbitrary
      ingestNumber        <- choose(1, 3)
      setup      =
        createLocationDir(repository.repo.root) >>
        Repositories.create(repository.repo)    >>
        fromDictionary(repository.repo, factsWithDictionary.dictionary, ImportOpts(Update, false)) >>
        importFacts(repository.repo, factsWithDictionary.facts).replicateM_(ingestNumber)
    } yield TemporaryRepositorySetup(repository, setup)
  }

  implicit def TemporaryRepository: Arbitrary[TemporaryRepository[Repository]] =
    temporaryRepository(List(Posix, Hdfs, S3))

  /** create an arbitrary for specific location types */
  def temporaryRepository(types: List[TemporaryType]): Arbitrary[TemporaryRepository[Repository]] = Arbitrary {
    for {
      repositoryType <- oneOf(types)
    } yield createTemporaryRepository(repositoryType)
  }

  def importFacts(repository: Repository, facts: List[Fact]): ResultTIO[Unit] = for {
    factsetId <- Ingest.createNewFactsetId(repository)
    _         <- updateFeatureStore(repository, factsetId)
  } yield ()

}

object ArbitraryRepositories extends ArbitraryRepositories
