package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._

import com.ambiata.poacher.mr._

import org.apache.hadoop.io.{BytesWritable, IntWritable, NullWritable, Writable}

sealed trait MrFactConverter[K <: Writable, V <: Writable] {
  def convert(fact: MutableFact, key: K, value: V): Unit
}
/** Currently only used for testing */
sealed trait MrFactWriter[K <: Writable, V <: Writable] {
  def write(fact: Fact, key: K, value: V): MrFactConverter[K, V]
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
case class PartitionFactWriter() extends MrFactWriter[NullWritable, BytesWritable] {
  val serialiser = ThriftSerialiser()
  def write(fact: Fact, key: NullWritable, value: BytesWritable): MrFactConverter[NullWritable, BytesWritable] = {
    val bytes = serialiser.toByteViewUnsafe(fact.toThrift)
    value.setSize(bytes.length)
    System.arraycopy(bytes.bytes, 0, value.getBytes, 0, bytes.length)
    PartitionFactConverter(Partition(fact.namespace, fact.date))
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
case class MutableFactWriter() extends MrFactWriter[NullWritable, BytesWritable] {
  val serialiser = ThriftSerialiser()
  def write(fact: Fact, key: NullWritable, value: BytesWritable): MrFactConverter[NullWritable, BytesWritable] = {
    val bytes = serialiser.toByteViewUnsafe(fact.toNamespacedThrift)
    value.setSize(bytes.length)
    System.arraycopy(bytes.bytes, 0, value.getBytes, 0, bytes.length)
    MutableFactConverter()
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
case class NamespaceDateFactWriter() extends MrFactWriter[IntWritable, BytesWritable] {
  val serialiser = ThriftSerialiser()
  def write(fact: Fact, key: IntWritable, value: BytesWritable): MrFactConverter[IntWritable, BytesWritable] = {
    val bytes = serialiser.toByteViewUnsafe(fact.toThrift)
    value.setSize(bytes.length)
    System.arraycopy(bytes.bytes, 0, value.getBytes, 0, bytes.length)
    key.set(fact.date.underlying)
    NamespaceDateFactConverter(fact.namespace)
  }
}
