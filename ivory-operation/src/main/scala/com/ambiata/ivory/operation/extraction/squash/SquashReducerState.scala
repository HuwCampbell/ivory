package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.Entities
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import java.util.{Iterator => JIterator}
import org.apache.hadoop.io.{BytesWritable, NullWritable, Text}

object EntityIterator {

  /**
   * Encapsulates the logic of iterating over a set of ordered facts, processing each one and then 'emitting' when
   * a new entity is encountered or the end is reached.
   */
  def iterate[A](fact: MutableFact, mutator: FactByteMutator, iter: JIterator[BytesWritable])
                (initial: A, callback: EntityCallback[A]): Unit = {
    val state = new SquashReducerEntityState(null, Namespace("empty"))

    var value: A = initial
    while (iter.hasNext) {
      mutator.from(iter.next, fact)
      val newEntity = state.isNewEntity(fact)
      if (newEntity) {
        if (state.notFirstEntity) {
          callback.emit(state, value)
        }
        state.update(fact)
        value = callback.initialise(state)
      }
      callback.update(state, fact, value)
    }
    // Emit whatever we have left over
    callback.emit(state, value)
  }

  /** And I shall call it "The Wheel"! */
  trait EntityCallback[A] {

    def initialise(state: SquashReducerEntityState): A
    def update(state: SquashReducerEntityState, fact: MutableFact, value: A): A
    def emit(state: SquashReducerEntityState, value: A): Unit
  }
}

trait SquashReducerState[A] {
  def reduceAll(fact: MutableFact, emitFact: MutableFact, reducerPool: ReducerPool, mutator: FactByteMutator,
                iter: JIterator[BytesWritable], emitter: Emitter[NullWritable, A], out: A): Unit
}

class SquashReducerStateSnapshot(date: Date) extends SquashReducerState[BytesWritable] {

  def reduceAll(fact: MutableFact, emitFact: MutableFact, reducerPool: ReducerPool, mutator: FactByteMutator,
                iter: JIterator[BytesWritable], emitter: Emitter[NullWritable, BytesWritable], out: BytesWritable): Unit = {
    // Fact is null by default, and we want to re-use the same one
    emitFact.setFact(new ThriftFact)

    // This is easy for snapshot - we only need to create a single pool for a single date
    val reducers = reducerPool.compile(Array(date))(0)
    EntityIterator.iterate(fact, mutator, iter)((), new EntityIterator.EntityCallback[Unit] {
      def initialise(state: SquashReducerEntityState): Unit =
        SquashReducerState.clear(reducers)
      def update(state: SquashReducerEntityState, fact: MutableFact, value: Unit): Unit =
        SquashReducerState.update(fact, reducers)
      def emit(state: SquashReducerEntityState, value: Unit): Unit =
        SquashReducerState.emit(emitFact, mutator, reducers, emitter, out, state.namespace, state.entity, date)
    })
  }
}

/**
 * NOTE: Currently "dead code", but will soon be required for chord to function correctly.
 */
class SquashReducerStateChord(chord: Entities) extends SquashReducerState[BytesWritable] {

  class SquashChordReducerEntityState(var dates: Array[Int], var reducers: Int => List[(FeatureReduction, Reduction)])

  def reduceAll(fact: MutableFact, emitFact: MutableFact, reducerPool: ReducerPool, mutator: FactByteMutator,
                iter: JIterator[BytesWritable], emitter: Emitter[NullWritable, BytesWritable], out: BytesWritable): Unit = {
    val buffer = new StringBuilder
    // Fact is null by default, and we want to re-use the same one
    emitFact.setFact(new ThriftFact)

    // Just to save that extra allocation per-entity, otherwise we would just create it per entity
    val entityState = new SquashChordReducerEntityState(Array(), _ => Nil)
    EntityIterator.iterate(fact, mutator, iter)(entityState, new EntityIterator.EntityCallback[SquashChordReducerEntityState] {

      def initialise(state: SquashReducerEntityState): SquashChordReducerEntityState = {
        val dates = chord.entities.get(state.entity)
        entityState.reducers = reducerPool.compile(dates.map(Date.unsafeFromInt))
        entityState.dates = dates
        var i = 0
        while (i < dates.length) {
          SquashReducerState.clear(entityState.reducers(i))
          i += 1
        }
        entityState
      }

      def skipOldChords(fact: MutableFact, value: SquashChordReducerEntityState): Int = {
        var i = value.dates.length - 1
        while (i >= 0 && value.dates(i) < fact.date.underlying) {
          i -= 1
        }
        i
      }

      def update(state: SquashReducerEntityState, fact: MutableFact, value: SquashChordReducerEntityState): SquashChordReducerEntityState = {
        var i = skipOldChords(fact, value)
        // The rest of the chords may have a window that includes this fact
        while (i >= 0) {
          // The filtering of windowing happens this function call
          SquashReducerState.update(fact, value.reducers(i))
          i -= 1
        }
        value
      }

      def emit(state: SquashReducerEntityState, value: SquashChordReducerEntityState): Unit = {
        // Emit _all_ the features at the end of the entity
        var i = value.dates.length - 1
        while (i >= 0) {
          val date = Date.unsafeFromInt(value.dates(i))
          val entity = newEntityId(state.entity, date, buffer)
          SquashReducerState.emit(emitFact, mutator, value.reducers(i), emitter, out, state.namespace, entity, date)
          i -= 1
        }
      }
    })
  }

  val delim = ':'

  def newEntityId(entity: String, date: Date, buffer: StringBuilder): String = {
    buffer.setLength(0)
    buffer.append(entity)
    buffer.append(delim)
    buffer.append(date.hyphenated)
    buffer.toString()
  }
}

class SquashReducerStateDump(date: Date) extends SquashReducerState[Text] {

  def reduceAll(fact: MutableFact, emitFact: MutableFact, reducerPool: ReducerPool, mutator: FactByteMutator,
                iter: JIterator[BytesWritable], emitter: Emitter[NullWritable, Text], out: Text): Unit = {

    val reducers = reducerPool.compile(Array(date))(0)

    EntityIterator.iterate(fact, mutator, iter)((), new EntityIterator.EntityCallback[Unit] {
      def initialise(state: SquashReducerEntityState): Unit =
        SquashReducerState.clear(reducers)
      def update(state: SquashReducerEntityState, fact: MutableFact, value: Unit): Unit =
        SquashReducerState.update(fact, reducers)
      def emit(state: SquashReducerEntityState, value: Unit): Unit =
        reducers.foreach {
          // This has emit side-effects and _needs_ to be called for state-based features
          case (_, reduction) => reduction.save
        }
    })
  }
}

object SquashReducerState {

  val kout = NullWritable.get()

  def clear(reducers: Iterable[(FeatureReduction, Reduction)]): Unit =
    reducers.foreach(_._2.clear())

  def update(fact: MutableFact, reducers: Iterable[(FeatureReduction, Reduction)]): Unit = {
    reducers.foreach {
      case (_, reduction) =>
        reduction.update(fact)
    }
  }

  // Write out the final reduced values
  def emit(emitFact: MutableFact, mutator: FactByteMutator, reducers: Iterable[(FeatureReduction, Reduction)],
           emitter: Emitter[NullWritable, BytesWritable], out: BytesWritable, namespace: Namespace, entity: String,
           date: Date): Unit = {
    // Use emitFact here to avoid clobbering values in fact
    val nsfact = emitFact.toNamespacedThrift
    val tfact = nsfact.getFact
    nsfact.setNspace(namespace.name)
    // Arbitrarily setting the date to the snapshot date, but it could be anything and isn't important for output
    // https://github.com/ambiata/ivory/issues/293
    nsfact.setYyyyMMdd(date.int)
    tfact.setEntity(entity)
    tfact.unsetSeconds()
    reducers.foreach {
      case (fr, state) =>
        val value = state.save
        if (value != null) {
          tfact.setAttribute(fr.getSource)
          nsfact.setNspace(fr.getNs)
          tfact.setValue(value)
          mutator.mutate(nsfact, out)
          emitter.emit(kout, out)
        }
    }
  }
}

class SquashReducerEntityState(var entity: String, var namespace: Namespace) {

  def isNewEntity(fact: Fact): Boolean =
    fact.entity != entity

  def notFirstEntity: Boolean =
    entity != null

  def update(fact: Fact): Unit = {
    entity = fact.entity
    namespace = fact.namespaceUnsafe
  }
}
