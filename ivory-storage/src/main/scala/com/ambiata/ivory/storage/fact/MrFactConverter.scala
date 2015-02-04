package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._

import com.ambiata.poacher.mr._

import org.apache.hadoop.io.{BytesWritable, IntWritable, NullWritable, Writable}

sealed trait MrFactConverter[K <: Writable, V <: Writable] {
  def convert(fact: MutableFact, key: K, value: V): Unit
}

/** Partitioned sequence files where the key is null and the value is thrift serialised ThriftFact bytes.
    The partition is namespace/year/month/day */
case class PartitionFactConverter(partition: Partition) extends MrFactConverter[NullWritable, BytesWritable] {
  val deserialiser = ThriftSerialiser()
  def convert(fact: MutableFact, key: NullWritable, value: BytesWritable): Unit = {
    deserialiser.fromBytesViewUnsafe(fact.toThrift, value.getBytes, 0, value.getLength)
    fact.setNspace(partition.namespace.name)
    fact.setYyyyMMdd(partition.date.int)
    ()
  }
}

/** Sequence files where the key is null and the value is thrift serialised MutableFact bytes */
case class MutableFactConverter() extends MrFactConverter[NullWritable, BytesWritable] {
  val deserialiser = ThriftSerialiser()
  def convert(fact: MutableFact, key: NullWritable, value: BytesWritable): Unit = {
    deserialiser.fromBytesViewUnsafe(fact, value.getBytes, 0, value.getLength)
    ()
  }
}

/** Partitioned sequence files where the key is a date and the value is thrift serialised ThriftFact bytes.
    The partition is the namespace. */
case class NamespaceDateFactConverter(namespace: Namespace) extends MrFactConverter[IntWritable, BytesWritable] {
  val deserialiser = ThriftSerialiser()
  def convert(fact: MutableFact, key: IntWritable, value: BytesWritable): Unit = {
    deserialiser.fromBytesViewUnsafe(fact.toThrift, value.getBytes, 0, value.getLength)
    fact.setNspace(namespace.name)
    fact.setYyyyMMdd(key.get)
    ()
  }
}
