package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{Text, BytesWritable, NullWritable, IntWritable}
import org.apache.hadoop.fs.Path
import java.util.{Iterator => JIterator}
import scala.collection.JavaConverters._

object MockFactMutator {

  /** For testing MR code that deals with a stream of fact bytes */
  def run(facts: List[Fact])(f: (JIterator[BytesWritable], Emitter[NullWritable, BytesWritable], BytesWritable) => Unit): List[Fact] =
    runFatFactKeep(facts)(f)._1

  def runFatFactKeep[A](facts: List[Fact])(f: (JIterator[BytesWritable], Emitter[NullWritable, BytesWritable], BytesWritable) => A): (List[Fact], A) = {
    val serialiser = ThriftSerialiser()
    val emitter = TestEmitter[NullWritable, BytesWritable, Fact]((key, value) => {
      serialiser.fromBytesViewUnsafe(createMutableFact, value.getBytes, 0, value.getLength)
    })
    val result = iterateFactsAsBytes(facts)(iter => f(iter, emitter, Writables.bytesWritable(4096)))
    (emitter.emitted.toList, result)
  }

  def runThriftFactKeep[A](namespace: Namespace, facts: List[Fact])(f: (JIterator[BytesWritable], Emitter[IntWritable, BytesWritable], IntWritable, BytesWritable) => A): (List[Fact], A) = {
    val serialiser = ThriftSerialiser()
    val emitter = TestEmitter[IntWritable, BytesWritable, Fact]((key, value) => {
      val date = Date.unsafeFromInt(key.get)
      val tfact = new ThriftFact
      serialiser.fromBytesViewUnsafe(tfact, value.getBytes, 0, value.getLength)
      FatThriftFact(namespace.name, date, tfact)
    })
    val result = iterateFactsAsBytes(facts)(iter => f(iter, emitter, new IntWritable(0), Writables.bytesWritable(4096)))
    (emitter.emitted.toList, result)
  }

  def runText(facts: List[Fact])(f: (JIterator[BytesWritable], Emitter[NullWritable, Text], Text) => Unit): List[String] = {
    val emitter = TestEmitter[NullWritable, Text, String]((key, value) => {
      value.toString
    })
    iterateFactsAsBytes(facts)(iter => f(iter, emitter, new Text))
    emitter.emitted.toList
  }

  def iterateFactsAsBytes[A](facts: List[Fact])(f: JIterator[BytesWritable] => A): A = {
    // When in Rome. This is what Hadoop does
    val in = Writables.bytesWritable(4096)
    val serialiser = ThriftSerialiser()
    f(facts.toIterator.map {
      fact =>
        val bytes = serialiser.toBytes(fact.toNamespacedThrift)
        in.set(bytes, 0, bytes.length)
        in
    }.asJava)
  }
}
