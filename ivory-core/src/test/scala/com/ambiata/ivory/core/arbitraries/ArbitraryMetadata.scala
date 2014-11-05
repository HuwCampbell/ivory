package com.ambiata.ivory.core
package arbitraries

import org.scalacheck.{Gen, Arbitrary}
import org.scalacheck.Arbitrary._
import ArbitraryFeatures._
import ArbitraryValues._
import scalaz._, Scalaz._, scalacheck.ScalaCheckBinding._

/**
 * Arbitraries for generating metadata: FeatureStores, Commits, SnapshotIds,...
 */
trait ArbitraryMetadata {

  implicit def FeatureStoreIdArbitrary: Arbitrary[FeatureStoreId] =
    Arbitrary(arbitrary[Identifier].map(FeatureStoreId.apply))

  implicit def ModeArbitrary: Arbitrary[Mode] =
    Arbitrary(Gen.oneOf(Mode.State, Mode.Set))

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(Gen.oneOf(
      Gen.choose(1l, 0xffffffffl).map(x => Identifier.unsafe(x.toInt))
      , Gen.choose(1, 99999).map(x => Identifier.unsafeV1(x))
    ))

  def createIdentifiers(n: Int): List[Identifier] =
    (1 to n).scanLeft(Identifier.initial)((acc, _) => acc.next.get).toList

  implicit def IdentifierListArbitrary: Arbitrary[IdentifierList] =
    Arbitrary(Gen.choose(0, 200) map (n => IdentifierList(createIdentifiers(n))))

  implicit def SmallIdentifierListArbitrary: Arbitrary[SmallIdentifierList] =
    Arbitrary(Gen.choose(0, 20) map (n => SmallIdentifierList(createIdentifiers(n))))

  implicit def SmallFeatureStoreIdListArbitrary: Arbitrary[SmallFeatureStoreIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallFeatureStoreIdList(ids.ids.map(FeatureStoreId.apply))))

  implicit def SmallCommitIdListArbitrary: Arbitrary[SmallCommitIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallCommitIdList(ids.ids.map(CommitId.apply))))

  implicit def FeatureStoreArbitrary: Arbitrary[FeatureStore] = Arbitrary(for {
    storeId    <- arbitrary[FeatureStoreId]
    factsets   <- genFactsetList(Gen.choose(1, 3))
  } yield FeatureStore.fromList(storeId, factsets).get)

  implicit def DictionaryIdArbitrary: Arbitrary[DictionaryId] =
    Arbitrary(arbitrary[Identifier].map(DictionaryId.apply))

  implicit def CommitIdArbitrary: Arbitrary[CommitId] = Arbitrary(for {
    x <- arbitrary[Identifier]
  } yield CommitId(x))

  implicit def CommitArbitrary: Arbitrary[Commit] = Arbitrary(for {
    dictId     <- arbitrary[DictionaryId]
    fsid       <- arbitrary[FeatureStoreId]
  } yield Commit(dictId, fsid))

  implicit def SnapshotIdArbitrary: Arbitrary[SnapshotId] =
    Arbitrary(arbitrary[Identifier].map(SnapshotId.apply))

  implicit def SmallSnapshotIdListArbitrary: Arbitrary[SmallSnapshotIdList] =
    Arbitrary(arbitrary[SmallIdentifierList].map(ids => SmallSnapshotIdList(ids.ids.map(SnapshotId.apply))))

  implicit def FactsetIdArbitrary: Arbitrary[FactsetId] =
    Arbitrary(arbitrary[Identifier].map(id => FactsetId(id)))

  def genFactsetIds(ids: Gen[Int]): Gen[List[FactsetId]] =
    ids.map(createIdentifiers).map(ids => ids.map(FactsetId.apply))

  /* List of FactsetIds with a size between 0 and 10 */
  implicit def FactsetIdListArbitrary: Arbitrary[FactsetIdList] =
    Arbitrary(genFactsetIds(Gen.choose(0, 10)).map(FactsetIdList.apply))

  /* Generate a Factset with number of partitions up to n namespaces x n dates */
  def genFactset(factsetId: FactsetId, nNamespaces: Gen[Int], nDates: Gen[Int]): Gen[Factset] =
    genPartitions(nNamespaces, nDates).map(ps => Factset(factsetId, ps))

  /* Factset with up to 3 x 3 partitions */
  implicit def FactsetArbitrary: Arbitrary[Factset] =
    Arbitrary(for {
      id <- arbitrary[FactsetId]
      fs <- genFactset(id, Gen.choose(1, 3), Gen.choose(1, 3))
    } yield fs)

  /* Generate a list of Factset's, each with up to 3 x 3 partitions */
  def genFactsetList(size: Gen[Int]): Gen[List[Factset]] = for {
    ids <- genFactsetIds(size)
    fs  <- ids.traverse(id => genFactset(id, Gen.choose(1, 3), Gen.choose(1, 3)))
  } yield fs

  /* List of Factsets with size between 0 and 3. Each Factset has up to 3 x 3 partitions */
  implicit def FactsetListArbitrary: Arbitrary[FactsetList] =
    Arbitrary(genFactsetList(Gen.choose(0, 3)).map(FactsetList.apply))

  implicit def PartitionArbitrary: Arbitrary[Partition] = Arbitrary(for {
    ns      <- arbitrary[FeatureNamespace]
    date    <- arbitrary[Date]
  } yield Partition(ns.namespace, date))

  /* Generate a list of Partitions with the size up to n namespaces x n dates */
  def genPartitions(nNamespaces: Gen[Int], nDates: Gen[Int]): Gen[Partitions] = for {
    namespaces <- genFeatureNamespaces(nNamespaces)
    partitions <- namespaces.traverse(ns => genDates(nDates).map(_.map(d => Partition(ns.namespace, d))))
  } yield Partitions(partitions.flatten)

  /* Partitions with size up to 3 x 5 */
  implicit def PartitionsArbitrary: Arbitrary[Partitions] =
    Arbitrary(genPartitions(Gen.choose(1, 3), Gen.choose(1, 5)))
}

object ArbitraryMetadata extends ArbitraryMetadata

case class SmallFeatureStoreIdList(ids: List[FeatureStoreId])
case class SmallCommitIdList(ids: List[CommitId])
case class SmallSnapshotIdList(ids: List[SnapshotId])
case class FactsetIdList(ids: List[FactsetId])
case class IdentifierList(ids: List[Identifier])
case class SmallIdentifierList(ids: List[Identifier])


