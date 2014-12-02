package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.{ThriftFact, ThriftFactValue}
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.operation.extraction.reduction.Reduction

object SquashDump {

  /**
   * Return a dictionary that contains full concrete features that match, or the reduced set of concrete features with
   * matching virtual features.
   */
  def filterByConcreteOrVirtual(dictionary: Dictionary, features: Set[FeatureId]): Dictionary =
     DictionaryConcrete(dictionary.byConcrete.sources.flatMap {
       case (fid, cg) =>
         if (features.contains(fid)) List(fid -> cg)
         else {
           cg.virtual.filter(v => features.contains(v._1)) match {
             case Nil     => Nil
             case virtual => List(fid -> cg.copy(virtual = virtual))
           }
         }
     }).dictionary

  def lookupConcreteFromVirtual(dictionary: Dictionary, virtual: FeatureId): Option[FeatureId] =
    dictionary.byFeatureId.get(virtual).flatMap(_.fold((_, _) => None, (_, d) => Some(d.source)))

  /** To be used in combination with [[SquashReducerStateDump]] */
  def wrap(delim: Char, missing: String, fr: FeatureReduction, r: Reduction, emitter: String => Unit): Reduction =
    new Reduction {
      val buffer = new StringBuilder
      val emitFact = createMutableFact
      emitFact.setFact(new ThriftFact)

      override def clear(): Unit =
        r.clear()

      def skip(fact: Fact, reason: String): Unit =
        write(fact, "SKIP: " + reason)

      override def update(fact: Fact): Unit = {
        // Update the fact with the current reduction value
        r.update(fact)
        // NOTE: We're relying on save being idempotent/effect-free here
        // This is/should be true of all reductions _except_ StateReduction, which is why we wrap the _inner_
        // reduction elsewhere so dump returns the correct results
        val value = r.save
        emitFact.getFact.setValue(value)
        val s = if (value != null) Value.toStringWithStruct(emitFact.value, missing) else missing
        write(fact, s)
      }

      def write(fact: Fact, value: String) {
        buffer.clear()
        buffer.append(fact.entity)
        buffer.append(delim)
        buffer.append(fr.ns)
        buffer.append(delim)
        buffer.append(fr.source)
        buffer.append(delim)
        // Output the time first so it's easier to eyeball the original and reduced values
        buffer.append(fact.datetime.localIso8601)
        buffer.append(delim)
        TextEscaping.escapeAppend(delim, Value.toStringWithStruct(fact.value, missing), buffer)
        buffer.append(delim)
        TextEscaping.escapeAppend(delim, value, buffer)
        emitter(buffer.toString())
      }

      override def save: ThriftFactValue =
        r.save
    }
}
