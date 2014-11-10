package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._
import org.scalacheck._


object GenIdentifier {
  def dictionary: Gen[DictionaryId] =
    identifier.map(DictionaryId.apply)

  def feature: Gen[FeatureId] = for {
    ns <- GenString.name
    name <- GenString.sensible
  } yield FeatureId(ns, name)

  // In many cases bad/strange things happen when using the same FeatureId for different Definitions
  def featureUnique(other: Set[FeatureId]): Gen[FeatureId] =
    feature.retryUntil(!other.contains(_))

  def identifier: Gen[Identifier] =
    Gen.choose(0, Int.MaxValue).map(Identifier.unsafe)

  def identifiers: Gen[List[Identifier]] =
    Gen.choose(0, 20).map(identifiersTo)

  def factset: Gen[FactsetId] =
    identifier.map(FactsetId.apply)

  def factsets: Gen[List[FactsetId]] =
    identifiers.map(_.map(FactsetId.apply))

  def commit: Gen[CommitId] =
    identifier.map(CommitId.apply)

  def commits: Gen[List[CommitId]] =
    identifiers.map(_.map(CommitId.apply))

  def store: Gen[FeatureStoreId] =
    identifier.map(FeatureStoreId.apply)

  def stores: Gen[List[FeatureStoreId]] =
    identifiers.map(_.map(FeatureStoreId.apply))

  def snapshot: Gen[SnapshotId] =
    identifier.map(SnapshotId.apply)

  def snapshots: Gen[List[SnapshotId]] =
    identifiers.map(_.map(SnapshotId.apply))

  def version: Gen[IvoryVersion] =
    Gen.identifier.map(IvoryVersion.apply)

  def identifiersTo(n: Int): List[Identifier] =
    (1 to n).scanLeft(Identifier.initial)((acc, _) => acc.next.get).toList
}
