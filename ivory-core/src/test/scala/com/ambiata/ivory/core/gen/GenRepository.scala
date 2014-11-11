package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._

import org.scalacheck._

import scalaz.{Name => _, Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._


object GenRepository {
  def datasets: Gen[Datasets] = for {
    n <- Gen.sized(z => Gen.choose(1, math.min(z, 20)))
    d <- Gen.listOfN(n, dataset)
  } yield Datasets(Prioritized.fromList(d).get)

  def dataset: Gen[Dataset] =
    Gen.oneOf(
      factset.map(FactsetDataset.apply)
    , snapshot.map(SnapshotDataset.apply)
    )

  def store: Gen[FeatureStore] = for {
    s <- GenIdentifier.store
    f <- factsets
  } yield FeatureStore.fromList(s, f).get

  def commit: Gen[Commit] = for {
    d <- GenIdentifier.dictionary
    s <- GenIdentifier.store
    c <- Gen.option(GenIdentifier.repositoryConfigId)
  } yield Commit(d, s, c)

  def factset: Gen[Factset] =
    GenIdentifier.factset.flatMap(factsetWith)

  def factsetWith(factsetId: FactsetId): Gen[Factset] =
    partitions.map(Factset(factsetId, _))

  def factsets: Gen[List[Factset]] = for {
    f <- GenIdentifier.factsets
    r <- f.traverse(factsetWith)
  } yield r

  def snapshot: Gen[Snapshot] = for {
    i <- GenIdentifier.snapshot
    d <- GenDate.date
    s <- store
  } yield Snapshot(i, d, s)

  def partition: Gen[Partition] = for {
    n <- GenString.name
    d <- GenDate.date
  } yield Partition(n, d)

  def partitions: Gen[List[Partition]] = for {
    n <- Gen.choose(1, 3)
    d <- Gen.choose(1, 5)
    p <- partitionsOf(n, d)
  } yield p

  /* Generate a list of Partitions with the size up to n namespaces x n dates */
  def partitionsOf(nNamespaces: Int, nDates: Int): Gen[List[Partition]] = for {
    // Make sure we generate distinct namespaces here, so that the dates below are actually distinct
    namespaces <- Gen.listOfN(nNamespaces, GenString.name).map(_.distinct)
    partitions <- namespaces.traverse(namespace => for {
      d <- Gen.listOfN(nDates * 2, GenDate.date)
    } yield d.distinct.map(Partition(namespace, _)))
  } yield partitions.flatten

  def repositoryConfig: Gen[RepositoryConfig] =
    GenDate.zone.map(RepositoryConfig.apply)
}
