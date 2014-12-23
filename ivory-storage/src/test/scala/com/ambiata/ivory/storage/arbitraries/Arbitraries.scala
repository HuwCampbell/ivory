package com.ambiata.ivory.storage.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.fact.{FactsetVersionTwo, FactsetVersionOne, FactsetVersion}
import com.ambiata.ivory.storage.manifest.{VersionManifest, SnapshotManifest}
import com.ambiata.notion.core.TemporaryType
import com.ambiata.notion.core.TemporaryType.{Hdfs, S3, Posix}
import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary._
import scalaz._, Scalaz._

trait Arbitraries {
  val awsEnabled = sys.env.contains("FORCE_AWS") || sys.env.contains("AWS_ACCESS_KEY")
  // This is a little dodgy, but means that property tests can be run on Travis without having AWS access
  if (!awsEnabled) {
    println("WARNING: AWS has been disabled for this build")
  }

  implicit def FactsetVersionArbitrary: Arbitrary[FactsetVersion] =
    Arbitrary(Gen.oneOf(FactsetVersionOne, FactsetVersionTwo))

  // TODO We should expose this from notion when we add the 'tool' dependency support
  implicit def StoreTypeArbitrary: Arbitrary[TemporaryType] =
    Arbitrary(if (awsEnabled) Gen.oneOf(Posix, S3, Hdfs) else Gen.oneOf(Posix, Hdfs))

  implicit def SnapshotManifestArbitrary: Arbitrary[SnapshotManifest] =
    Arbitrary(for {
      id <- arbitrary[SnapshotId]
      date <- arbitrary[Date]
      storeOrCommit <- Gen.frequency(1 -> arbitrary[FeatureStoreId].map(_.left), 9 -> arbitrary[CommitId].map(_.right))
    } yield SnapshotManifest.createDeprecated(storeOrCommit, id, SnapshotFormat.V1, date))
}

object Arbitraries extends Arbitraries
