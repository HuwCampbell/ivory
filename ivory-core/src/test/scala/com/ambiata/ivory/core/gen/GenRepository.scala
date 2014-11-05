package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._

import org.joda.time.DateTimeZone
import org.scalacheck._

import scalaz.{Name => _, Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._


object GenRepository {
  def store: Gen[FeatureStore] = for {
    storeId    <- GenIdentifier.store
    factsets   <- GenRepository.factsets
  } yield FeatureStore.fromList(storeId, factsets).get

  def commit: Gen[Commit] = for {
    d <- GenIdentifier.dictionary
    s <- GenIdentifier.store
  } yield Commit(d, s)

  def factset: Gen[Factset] =
    GenIdentifier.factset.flatMap(factsetWith)

  def factsetWith(factsetId: FactsetId): Gen[Factset] =
    partitions.map(Factset(factsetId, _))

  def factsets: Gen[List[Factset]] = for {
    f <- GenIdentifier.factsets
    r <- f.traverse(factsetWith)
  } yield r

  def partition: Gen[Partition] = for {
    n <- GenString.name
    d <- GenDate.date
  } yield Partition(n, d)

  def partitions: Gen[Partitions] = for {
    n <- Gen.choose(1, 3)
    d <- Gen.choose(1, 5)
    p <- partitionsOf(n, d)
  } yield p

  /* Generate a list of Partitions with the size up to n namespaces x n dates */
  def partitionsOf(nNamespaces: Int, nDates: Int): Gen[Partitions] = for {
    namespaces <- Gen.listOfN(nNamespaces, GenString.name)
    partitions <- namespaces.traverse(namespace => for {
      d <- Gen.listOfN(nDates * 2, GenDate.date)
    } yield d.distinct.map(Partition(namespace, _)))
  } yield Partitions(partitions.flatten)
}
