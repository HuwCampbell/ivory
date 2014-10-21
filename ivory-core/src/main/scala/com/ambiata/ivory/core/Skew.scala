package com.ambiata.ivory.core

import com.ambiata.mundane.io.BytesQuantity

/**
 * Calculation to work out optimal groupings.
 */
object Skew {

  /**
   *  given a list of namespaces and their data size, calculate the reduce buckets for each namespace/feature
   *
   *  once a reducer reaches the "optimal" size, we create a new reducer
   *
   *  @return the number of reducers
   *          a list of (namespace, feature id,
   */
  def calculate(dictionary: Dictionary, namespaces: List[(Name, BytesQuantity)], optimal: BytesQuantity): (Int, List[(Name, String, FeatureReducerOffset)]) =
    namespaces.foldLeft(0 -> List[(Name, String, FeatureReducerOffset)]()) { case ((allocated, acc), (namespace, size)) =>
      // Only consider the concrete features
      val features = dictionary.forNamespace(namespace).byConcrete.sources.keys.map(_.name).toList
      val potential = (size.toBytes.value / optimal.toBytes.value).toInt + 1
      // For this namespace, what is the proportion of the reducers that _each_ feature gets (rounding up to 1)
      val proportion = Math.max(1, potential / features.size)
      // Remove any used excess count if we have more reducers than features and they don't divide evenly
      // Alternatively we could give it (arbitrarily) to one of the features
      val excessCount = if (potential <= features.size) 0 else potential % features.size

      val x = features.zipWithIndex.map { case (feature, idx) =>
        (namespace, feature, FeatureReducerOffset((allocated + (idx * proportion % potential)).toShort, proportion.toShort))
      }
      (allocated + potential - excessCount, x ::: acc)
    }

}
