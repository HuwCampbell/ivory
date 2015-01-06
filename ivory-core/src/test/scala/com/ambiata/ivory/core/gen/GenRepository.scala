package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._

import org.scalacheck._, Arbitrary.arbitrary

import scalaz.{Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._


object GenRepository {
  def size: Gen[Long] =
    Gen.choose(0L, Long.MaxValue)

  def bytes: Gen[Bytes] =
    size.map(Bytes.apply)

  def sized[A: Arbitrary]: Gen[Sized[A]] = for {
    s <- bytes
    v <- arbitrary[A]
  } yield Sized(v, s)

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
    id <- GenIdentifier.commit
    d <- GenDictionary.identified
    s <- store
    c <- Gen.option(identifiedRepositoryConfig)
  } yield Commit(id, d, s, c)

  def commitMetadata: Gen[CommitMetadata] = for {
    d <- GenIdentifier.dictionary
    s <- GenIdentifier.store
    c <- Gen.option(GenIdentifier.repositoryConfigId)
  } yield CommitMetadata(d, s, c)

  def factset: Gen[Factset] =
    GenIdentifier.factset.flatMap(factsetWith)

  def factsetWith(factsetId: FactsetId): Gen[Factset] = for {
    ps <- partitions
    ss <- ps.traverse(p => bytes.map(s => Sized(p, s)))
  } yield Factset(factsetId, FactsetFormat.V2, ss)

  def factsets: Gen[List[Factset]] = for {
    f <- GenIdentifier.factsets
    r <- f.traverse(factsetWith)
  } yield r

  def snapshot: Gen[Snapshot] = for {
    i <- GenIdentifier.snapshot
    x <- GenDate.date
    s <- store
    d <- GenDictionary.identified
    b <- bytes
  } yield Snapshot(i, x, s, d.some, b)

  def partition: Gen[Partition] = for {
    n <- GenString.namespace
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
    namespaces <- Gen.listOfN(nNamespaces, GenString.namespace).map(_.distinct)
    partitions <- namespaces.traverse(namespace => for {
      d <- Gen.listOfN(nDates * 2, GenDate.date)
    } yield d.distinct.map(Partition(namespace, _)))
  } yield partitions.flatten

  def repositoryConfig: Gen[RepositoryConfig] =
    GenDate.zone.map(RepositoryConfig(MetadataVersion.V1, _))

  def identifiedRepositoryConfig: Gen[Identified[RepositoryConfigId, RepositoryConfig]] =
    GenIdentifier.identified(Arbitrary(GenIdentifier.repositoryConfigId), Arbitrary(repositoryConfig))
}
