package com.ambiata.ivory.operation.extraction.squash

import com.ambiata.ivory.core._

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
}
