package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.poacher.mr._
import org.apache.hadoop.io.{Text, BytesWritable, NullWritable}
import java.util.{Iterator => JIterator}
import scala.collection.JavaConverters._

object MockFactMutator {

  /** For testing MR code that deals with a stream of fact bytes */
  def run(facts: List[Fact])(f: (JIterator[BytesWritable], FactByteMutator, Emitter[NullWritable, BytesWritable], BytesWritable) => Unit): List[Fact] = {
    val outFacts = new collection.mutable.ListBuffer[Fact]
    val serialiser = ThriftSerialiser()
    val emitter = new Emitter[NullWritable, BytesWritable] {
      def emit(key: NullWritable, value: BytesWritable): Unit = {
        outFacts += serialiser.fromBytesViewUnsafe(createMutableFact, value.getBytes, 0, value.getLength)
        ()
      }
    }
    iterateFactsAsBytes(facts)(iter => f(iter, new FactByteMutator, emitter, Writables.bytesWritable(4096)))
    outFacts.toList
  }

  def runText(facts: List[Fact])(f: (JIterator[BytesWritable], Emitter[NullWritable, Text], Text) => Unit): List[String] = {
    val lines = new collection.mutable.ListBuffer[String]
    val emitter = new Emitter[NullWritable, Text] {
      def emit(key: NullWritable, value: Text): Unit = {
        lines += value.toString
        ()
      }
    }
    iterateFactsAsBytes(facts)(iter => f(iter, emitter, new Text))
    lines.toList
  }

  def iterateFactsAsBytes[A](facts: List[Fact])(f: JIterator[BytesWritable] => Unit): Unit = {
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
