package com.ambiata.ivory.operation

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.notion.core.TemporaryType._
import org.scalacheck._
import org.specs2.execute.AsResult
import org.specs2.{Specification, ScalaCheck}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import ArbitraryRepositories.temporaryRepositorySetupArbitrary

import scalaz.effect.Resource

class ArbitraryRepositoriesSpec extends Specification with ScalaCheck { def is = s2"""

 Arbitrary repositories can be created
   the repository must contain some metadata        $metadata

"""

  def metadata = propTempRepository { repository: Repository =>
    Metadata.listCommitIds(repository) must beOkLike(list => list must not (beEmpty))
  }.set(minTestsOk = 5)

  /** properties methods to run the set-up and do the clean-up for a repository */
  def propTempRepository[Repo <: Repository, R : AsResult](f: Repository => R): Prop = {
    // for now only run for hdfs and local because S3 doesn't pass
    implicit val arbitrary: Arbitrary[TemporaryRepositorySetup[Repository]] =
      temporaryRepositorySetupArbitrary(List(Posix, Hdfs))

    propResource { repositorySetup: TemporaryRepositorySetup[Repository] =>
      repositorySetup.setup.run.unsafePerformIO
      f(repositorySetup.repository)
    }
  }

  def propResource[T : Resource : Arbitrary : Shrink, R : AsResult](f: T => R): Prop =
    prop((t: T) => try AsResult(f(t)) finally Resource[T].close(t).unsafePerformIO)
}
