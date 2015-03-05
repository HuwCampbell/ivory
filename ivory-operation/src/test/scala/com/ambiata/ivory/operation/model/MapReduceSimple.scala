package com.ambiata.ivory.operation.model

import scalaz._, Scalaz._

/**
 * Represents a simple model of what a typical map/reduce will do,
 * allowing for an in-memory (and performant agnostic) copy of the live/raw version for testing purposes.
 */
trait MapReduceSimple[I, O, C] {

  type M
  type K
  type S
  def secondary: Order[S]

  /** Do a full map/reduce with grouping and sorting */
  def run(values: List[I], conf: C): List[O] =
    map(values, conf)
      .groupBy1(_._1).toList
      .flatMap(x => reduce(x._1, x._2.sortBy(_._2)(secondary).map(_._3), conf))

  def map(values: List[I], conf: C): List[(K, S, M)]
  def reduce(key: K, values: NonEmptyList[M], conf: C): List[O]
}
