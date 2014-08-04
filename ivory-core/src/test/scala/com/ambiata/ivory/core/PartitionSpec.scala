package com.ambiata.ivory.core

import org.specs2._, matcher._, specification._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import java.io.File
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz._, Scalaz._

class PartitionSpec extends Specification with ScalaCheck { def is = s2"""

Partition Tests
----------

  Paths between two dates       $between
  Paths before a date           $before
  Paths after a date            $after

"""

  def between = prop((partitions: SmallPartitionList, dates: TwoDifferentDates) => {
    val ps = partitions.partitions
    val expected = ps.filter(p => p.date.isAfterOrEqual(dates.earlier) && p.date.isBeforeOrEqual(dates.later))

    Partitions.pathsBetween(ps, dates.earlier, dates.later) must_== expected
  })

  def before = prop((partitions: SmallPartitionList, date: Date) => {
    val ps = partitions.partitions

    Partitions.pathsBeforeOrEqual(ps, date) must_== ps.filter(_.date.isBeforeOrEqual(date))
  })

  def after = prop((partitions: SmallPartitionList, date: Date) => {
    val ps = partitions.partitions

    Partitions.pathsAfterOrEqual(ps, date) must_== ps.filter(_.date.isAfterOrEqual(date))
  })
}
