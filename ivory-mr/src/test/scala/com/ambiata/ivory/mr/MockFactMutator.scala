package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._

class MockFactMutator extends Mutator[MutableFact, MutableFact, Fact, Unit] with PipeMutator[MutableFact, Fact, Unit] with Emitter {

  val facts = collection.mutable.ListBuffer[Fact]()
  var last: MutableFact = null

  def from(in: Fact, fact: MutableFact): Unit = {
    val serialiser = ThriftSerialiser()
    // Be nice if there was a way in the thrift objects to do this "copy", but it doesn't matter
    serialiser.fromBytesUnsafe(fact, serialiser.toBytes(in.toNamespacedThrift))
    ()
  }

  def mutate(fact: MutableFact, out: Unit): Unit =
    last = new NamespacedThriftFact(fact.toNamespacedThrift) with NamespacedThriftFactDerived

  // We shouldn't need to take a copy here because they have promised not to mutate!
  def pipe(in: Fact, out: Unit): Unit =
    last = in.toNamespacedThrift

  def emit(): Unit = {
    facts += last
    ()
  }
}
