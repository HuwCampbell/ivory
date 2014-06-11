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
  def calculate(dictionary: Dictionary, namespaces: List[(String, BytesQuantity)], optimal: BytesQuantity): (Int, List[(String, String, Int)]) =
    namespaces.foldLeft(0 -> List[(String, String, Int)]()) { case ((allocated, acc), (namespace, size)) =>
      val features = dictionary.forNamespace(namespace).meta.keys.map(_.name).toList
      val count = features.size
      val potential = (size.toBytes.value / optimal.toBytes.value).toInt + 1
      
      val x = features.zipWithIndex.map { case (feature, idx) => 
        (namespace, feature, allocated + (idx % potential))
      }
      (allocated + math.min(potential, count) , x ::: acc)
    }

}
