package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.mr._
import com.ambiata.ivory.operation.extraction.reduction.Reduction
import java.util.{Iterator => JIterator}
import org.apache.hadoop.io.NullWritable

class SquashReducerState(date: Date) {

  def reduceAll[I, O](fact: MutableFact, emitFact: MutableFact, reducers: Iterable[(FeatureReduction, Reduction)],
                      mutator: FactMutator[I, O], iter: JIterator[I], emitter: Emitter[NullWritable, O], out: O) {
    val kout = NullWritable.get()
    var previousEntity: String = null
    var previousNamespace: String = null
    // Fact is null by default, and we want to re-use the same one
    emitFact.setFact(new ThriftFact)
     // Write out the final reduced values
    def emit() {
      // Use emitFact here to avoid clobbering values in fact
      val nsfact = emitFact.toNamespacedThrift
      val tfact = nsfact.getFact
      nsfact.setNspace(previousNamespace)
      // Arbitrarily setting the date to the snapshot date, but it could be anything and isn't important for pivot
      // https://github.com/ambiata/ivory/issues/293
      nsfact.setYyyyMMdd(date.int)
      tfact.setEntity(previousEntity)
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
    while (iter.hasNext) {
      mutator.from(iter.next, fact)
      if (previousEntity != fact.entity) {
        if (previousEntity != null) {
          emit()
        }
        reducers.foreach(_._2.clear())
        previousEntity = fact.entity
        previousNamespace = fact.namespaceUnsafe.name
      }
      reducers.foreach {
        case (fr, state) =>
          // We have captured the largest window, and we now need to filter by date per virtual feature
          if (fr.date <= fact.date.int) {
            state.update(fact)
          }
      }
    }
    // Emit whatever we have left over
    emit()
  }
}
