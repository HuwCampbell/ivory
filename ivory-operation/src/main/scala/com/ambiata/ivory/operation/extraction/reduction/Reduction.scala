package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift.ThriftFactValue
import com.ambiata.ivory.lookup.FeatureReduction
import com.ambiata.ivory.storage.metadata.DictionaryTextStorageV2

/**
 * Map-reduce reductions that represent a single feature gen.
 *
 * NOTE: For performance reasons only these are stateful, please design with care!!!
 */
trait Reduction {

  /** Reset the state of this reduction for the next series of facts */
  def clear(): Unit

  /** Update the state based on a single [[Fact]] */
  def update(f: Fact): Unit

  /** Return a final thrift value based on the current state, or null to filter out */
  def save: ThriftFactValue
}

object Reduction {

  def compile(fr: FeatureReduction): Option[Reduction] = for {
    exp      <- Expression.parse(fr.getExpression)
    encoding <- DictionaryTextStorageV2.parseEncoding(fr.getEncoding).toOption
    filter    = if (fr.isSetFilter) Some(Filter(fr.getFilter)) else None
    reduction = Reduction.fromExpression(exp)
  } yield filter.flatMap(FilterReducer.compile(_, encoding, reduction).toOption).getOrElse(reduction)

  def fromExpression(exp: Expression): Reduction = exp match {
    case Count  => new CountReducer()
    case Latest => new LatestReducer()
  }
}
