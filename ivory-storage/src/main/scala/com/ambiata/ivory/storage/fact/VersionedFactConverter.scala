package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.storage.legacy._

/** Version specific thrift converter */
sealed trait VersionedFactConverter {
  def convert(tfact: ThriftFact): Fact
}
case class VersionOneFactConverter(partition: Partition) extends VersionedFactConverter {
  def convert(tfact: ThriftFact): Fact =
    PartitionFactThriftStorageV1.createFact(partition, tfact)
}
case class VersionTwoFactConverter(partition: Partition) extends VersionedFactConverter {
  def convert(tfact: ThriftFact): Fact =
    PartitionFactThriftStorageV2.createFact(partition, tfact)
}
