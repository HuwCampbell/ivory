package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.Entities
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import java.util.{Iterator => JIterator}
import org.apache.hadoop.io.NullWritable

object EntityIterator {

  /**
   * Encapsulates the logic of iterating over a set of ordered facts, processing each one and then 'emitting' when
   * a new entity is encountered or the end is reached.
   */
  def iterate[I, O, A](fact: MutableFact, mutator: FactMutator[I, O], iter: JIterator[I])
                      (initial: A, callback: EntityCallback[A]): Unit = {
    val state = new SquashReducerEntityState(null, Name("empty"))

    var value: A = initial
    while (iter.hasNext) {
      mutator.from(iter.next, fact)
      if (state.isNewEntity(fact)) {
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

class SquashReducerState(date: Date) {

  def reduceAll[I, O](fact: MutableFact, emitFact: MutableFact, reducers: Iterable[(FeatureReduction, Reduction)],
                      mutator: FactMutator[I, O], iter: JIterator[I], emitter: Emitter[NullWritable, O], out: O): Unit = {
    // Fact is null by default, and we want to re-use the same one
    emitFact.setFact(new ThriftFact)

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
class SquashChordReducerState(chord: Entities) {

  class SquashChordReducerEntityState(var dates: Array[Int], var reducers: Int => List[(FeatureReduction, Reduction)])

  def reduceAll[I, O](fact: MutableFact, emitFact: MutableFact, reducerLookup: Array[Int] => Int => List[(FeatureReduction, Reduction)],
                      mutator: FactMutator[I, O], iter: JIterator[I], emitter: Emitter[NullWritable, O], out: O): Unit = {
    val buffer = new StringBuilder
    // Fact is null by default, and we want to re-use the same one
    emitFact.setFact(new ThriftFact)

    // Just to save that extra allocation per-entity, otherwise we would just create it per entity
    val entityState = new SquashChordReducerEntityState(Array(), _ => Nil)
    EntityIterator.iterate(fact, mutator, iter)(entityState, new EntityIterator.EntityCallback[SquashChordReducerEntityState] {

      def initialise(state: SquashReducerEntityState): SquashChordReducerEntityState = {
        val dates = chord.entities.get(state.entity)
        entityState.reducers = reducerLookup(dates)
        entityState.dates = dates
        var i = 0
        while (i < dates.length) {
          SquashReducerState.clear(entityState.reducers(i))
          i += 1
        }
        entityState
      }

      def update(state: SquashReducerEntityState, fact: MutableFact, value: SquashChordReducerEntityState): SquashChordReducerEntityState = {
        var i = value.dates.length - 1
        // Skip the old chords
        while (i >= 0 && value.dates(i) < fact.date.underlying) {
          i -= 1
        }
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

object SquashReducerState {

  val kout = NullWritable.get()

  def clear(reducers: Iterable[(FeatureReduction, Reduction)]): Unit =
    reducers.foreach(_._2.clear())

  def update(fact: MutableFact, reducers: Iterable[(FeatureReduction, Reduction)]): Unit = {
    reducers.foreach {
      case (fr, state) =>
        // We have captured the largest window, and we now need to filter by date per virtual feature
        if (fr.date <= fact.date.int) {
          state.update(fact)
        }
    }
  }

  // Write out the final reduced values
  def emit[I, O](emitFact: MutableFact, mutator: FactMutator[I, O], reducers: Iterable[(FeatureReduction, Reduction)],
                 emitter: Emitter[NullWritable, O], out: O, namespace: Name, entity: String, date: Date): Unit = {
    // Use emitFact here to avoid clobbering values in fact
    val nsfact = emitFact.toNamespacedThrift
    val tfact = nsfact.getFact
    nsfact.setNspace(namespace.name)
    // Arbitrarily setting the date to the snapshot date, but it could be anything and isn't important for pivot
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

class SquashReducerEntityState(var entity: String, var namespace: Name) {

  def isNewEntity(fact: Fact): Boolean =
    fact.entity != entity

  def notFirstEntity: Boolean =
    entity != null

  def update(fact: Fact): Unit = {
    entity = fact.entity
    namespace = fact.namespaceUnsafe
  }
}
