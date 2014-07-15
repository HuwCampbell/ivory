package com.ambiata.ivory.benchmark

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.extract._
import com.ambiata.ivory.mr.Writables
import com.google.caliper._
import org.apache.hadoop.io._
import org.apache.thrift.protocol.TCompactProtocol
import org.apache.thrift.{TSerializer, TDeserializer}
import scalaz._, Scalaz._

object SnapshotMutationsBenchApp extends App {
  Runner.main(classOf[SnapshotMutationsBench], args)
}

/*
 * Benchmarking some different implementations of the reduce side snapshot
 * mutations.
 *
 * The slowest is the MutableState implementation, the others all score
 * around the same.
 */
case class SnapshotMutationsBench() extends SimpleScalaBenchmark {
  val serializer = new TSerializer(new TCompactProtocol.Factory)
  val deserializer = new TDeserializer(new TCompactProtocol.Factory)

  def date(): Date = Date(2014, 1, 1)
  def time(): Time = Time(0)

  val testFact = Fact.newFact("eid", "ns", "fid", date, time, StringValue("abc"))
  val testBytes = serializer.serialize(new PriorityTag(1, java.nio.ByteBuffer.wrap(serializer.serialize(testFact.toNamespacedThrift))))

  /*
   * Marks suggestion
   */
  def time_reducerOther(n: Int) = {
    case class ReduceState(var latestContainer: PriorityTag, var latestDate: Long, var isTombstone: Boolean) {
      def accept(container: PriorityTag, nextDate: Long): Boolean =
        latestContainer == null || nextDate > latestDate || (nextDate == latestDate && container.getPriority < latestContainer.getPriority)

      def save(fact: Fact, container: PriorityTag, nextDate: Long): Unit = {
        latestContainer = container.deepCopy
        latestDate = nextDate
        isTombstone = fact.isTombstone
      }

      def write(vout: BytesWritable, commit: () => Unit): Unit =
        if (!isTombstone) {
          vout.set(latestContainer.getBytes, 0, latestContainer.getBytes.length)
          commit()
        }
    }

    val container = new PriorityTag
    val fact = new NamespacedThriftFact with NamespacedThriftFactDerived
    val vout = Writables.bytesWritable(4096)
    var out: BytesWritable = null
    val state = ReduceState(null, 0l, true)
    repeat[BytesWritable](n) {
      deserializer.deserialize(container, testBytes)
      deserializer.deserialize(fact, container.getBytes) // explain why deserialize twice
      val nextDate = fact.datetime.long
      if (state.accept(container, nextDate))
        state.save(fact, container, nextDate)
      state.write(vout, () => out = vout)
      out
    }
  }

  /*
   * Original implementation
   */
  def time_original(n: Int) = {
    val container = new PriorityTag
    val fact = new NamespacedThriftFact with NamespacedThriftFactDerived
    var latestContainer: PriorityTag = null
    var latestDate = 0l
    var isTombstone = true
    val vout = Writables.bytesWritable(4096)
    var out: BytesWritable = null
    repeat[BytesWritable](n) {
      deserializer.deserialize(container, testBytes)
      deserializer.deserialize(fact, container.getBytes) // explain why deserialize twice
      val nextDate = fact.datetime.long
      // move the if statement to a function
      if(latestContainer == null || nextDate > latestDate || (nextDate == latestDate && container.getPriority < latestContainer.getPriority)) {
        // change to state.set (1 line)
        latestContainer = container.deepCopy
        latestDate = nextDate
        isTombstone = fact.isTombstone
      }
      // change to state.write
      if(!isTombstone) {
        vout.set(latestContainer.getBytes, 0, latestContainer.getBytes.length)
        out = vout
      }
      out
    }
  }

  /*
   * All pieces from MutableState implementation without the function composition
   * (compare to MutableState implementation to check how much overhead there is)
   */
  def time_reducerWithoutComposition(n: Int) = {
    val s1 = new PriorityTag

    var s2_fact = new NamespacedThriftFact with NamespacedThriftFactDerived
    var s2_pt = new PriorityTag

    var s3_pt = new PriorityTag
    var s3_date = 0l
    var s3_isTombstone = true
    var s3_entity = ""

    val s4 = Writables.bytesWritable(4096)
    var out: BytesWritable = null

    repeat[BytesWritable](n) {
      // First
      s1.clear()
      deserializer.deserialize(s1, testBytes)

      // Second
      s2_fact.clear()
      deserializer.deserialize(s2_fact, s1.getBytes)
      s2_pt = s1

      // Third
      val nextDate = s2_fact.datetime.long
      if(s3_entity != s2_fact.entity || nextDate > s3_date || (nextDate == s3_date && s2_pt.getPriority < s3_pt.getPriority)) {
        s3_entity = s2_fact.entity
        s3_pt = s2_pt.deepCopy
        s3_date = nextDate
        s3_isTombstone = s2_fact.isTombstone
      }

      // Fourth
      if(s3_isTombstone)
        s4.setSize(0)
      else
        s4.set(s3_pt.getBytes, 0, s3_pt.getBytes.length)
      out = s4
      out
    }
  }

  /*
   * MutableState implementation
   */
  def time_reducerWithComposition(n: Int) = {
    case class MutableState[-A, S](val state: S, doMutation: (A, S) => Unit) {
      def mutate(a: A) {
        doMutation(a, state)
      }

      /* Return a MutableState which mutates the state of this object then passes it to the next MutableState */
      def andThen[X](next: MutableState[S, X]): MutableState[A, X] = {
        MutableState(next.state, (a: A, x: X) => { doMutation(a, state); next.doMutation(state, x) })
      }

      def writeWith(f: S => Unit): Unit =
        f(state)
    }

    def outputValueState(capacity: Int): MutableState[Array[Byte], BytesWritable] =
      thriftPriorityTagState andThen
      factPriorityTagState   andThen
      latestPriorityTagState andThen
      latestPriorityTagBytesState(capacity)

    def thriftPriorityTagState: MutableState[Array[Byte], PriorityTag] = {
      val deserializer = new TDeserializer(new TCompactProtocol.Factory)
      MutableState(
        new PriorityTag,
        (bytes: Array[Byte], state: PriorityTag) => {
          state.clear()
          deserializer.deserialize(state, bytes)
        })
    }

    case class MutableFactPriorityTag(var fact: NamespacedThriftFact with NamespacedThriftFactDerived, var priorityTag: PriorityTag)

    def factPriorityTagState: MutableState[PriorityTag, MutableFactPriorityTag] = {
      val deserializer = new TDeserializer(new TCompactProtocol.Factory)
      MutableState(
        MutableFactPriorityTag(new NamespacedThriftFact with NamespacedThriftFactDerived, new PriorityTag),
        (pt: PriorityTag, state: MutableFactPriorityTag) => {
          state.fact.clear()
          deserializer.deserialize(state.fact, pt.getBytes)
          state.priorityTag = pt
        })
    }

    case class MutableLatestPriorityTag(var priorityTag: PriorityTag, var date: Long, var isTombstone: Boolean, var entity: String)

    def latestPriorityTagState: MutableState[MutableFactPriorityTag, MutableLatestPriorityTag] = {
      MutableState(
        MutableLatestPriorityTag(new PriorityTag, 0l, true, ""),
        (factPriority: MutableFactPriorityTag, state: MutableLatestPriorityTag) => {
          val nextDate = factPriority.fact.datetime.long
          if(state.entity != factPriority.fact.entity || nextDate > state.date || (nextDate == state.date && factPriority.priorityTag.getPriority < state.priorityTag.getPriority)) {
            state.entity = factPriority.fact.entity
            state.priorityTag = factPriority.priorityTag.deepCopy
            state.date = nextDate
            state.isTombstone = factPriority.fact.isTombstone
          }
        })
    }

    def latestPriorityTagBytesState(capacity: Int): MutableState[MutableLatestPriorityTag, BytesWritable] =
      MutableState(Writables.bytesWritable(capacity), (m: MutableLatestPriorityTag, state: BytesWritable) => {
        if(m.isTombstone)
          state.setSize(0)
        else
          state.set(m.priorityTag.getBytes, 0, m.priorityTag.getBytes.length)
      })

    var bw: BytesWritable = null
    val mutable = outputValueState(4096)
    repeat[BytesWritable](n) {
      mutable.mutate(testBytes)
      mutable.writeWith(s => bw = s)
      bw
    }
  }
}
