package com.ambiata.ivory.storage.plan

import com.ambiata.ivory.core._

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import com.ambiata.ivory.storage.Arbitraries._

object DatasetSpec extends Specification with ScalaCheck { def is = s2"""

  Can filter FactsetDataset partitions:
    Before a given date                        $before
    Before or equal to a given date            $beforeOrEqual
    After a given date                         $after
    After or equal to a given date             $afterOrEqual
    Between two dates inclusive                $betweenInclusive
    Between two dates exclusive                $betweenExclusive
    Custom                                     $customFilter
                                               """

  def before = prop((d: Date, dataset: FactsetDataset) =>
    dataset.partitionsBefore(d) must_== dataset.copy(partitions = dataset.partitions.filter(_.date isBefore d)))

  def beforeOrEqual = prop((d: Date, dataset: FactsetDataset) =>
    dataset.partitionsBeforeOrEqual(d) must_== dataset.copy(partitions = dataset.partitions.filter(_.date isBeforeOrEqual d)))

  def after = prop((d: Date, dataset: FactsetDataset) =>
    dataset.partitionsAfter(d) must_== dataset.copy(partitions = dataset.partitions.filter(_.date isAfter d)))

  def afterOrEqual = prop((d: Date, dataset: FactsetDataset) =>
    dataset.partitionsAfterOrEqual(d) must_== dataset.copy(partitions = dataset.partitions.filter(_.date isAfterOrEqual d)))

  def betweenInclusive = prop((start: Date, end: Date, dataset: FactsetDataset) => {
    val expected = dataset.copy(partitions = dataset.partitions.filter(p => p.date.isAfterOrEqual(start) && p.date.isBeforeOrEqual(end)))
    dataset.partitionsBetweenInclusive(start, end) must_== expected
  })

  def betweenExclusive = prop((start: Date, end: Date, dataset: FactsetDataset) => {
    val expected = dataset.copy(partitions = dataset.partitions.filter(p => p.date.isAfter(start) && p.date.isBefore(end)))
    dataset.partitionsBetweenExclusive(start, end) must_== expected
  })

  def customFilter = prop((ns: FeatureNamespace, dataset: FactsetDataset) => {
    val expected = dataset.copy(partitions = dataset.partitions.filter(_.namespace == ns.namespace))
    dataset.filter(_.namespace == ns.namespace) must_== expected
  })
}
