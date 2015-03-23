package com.ambiata.ivory.operation.extraction.reduction

/** Handle the latest of a single struct value */
class UnionReducer[A] extends ReductionFold[Set[A], List[A], List[A]] {

  def initial: Set[A] = Set[A]()

  def fold(a: Set[A], value: List[A]): Set[A] =
    a.union(value.toSet)

  def tombstone(a: Set[A]): Set[A] =
    a

  def aggregate(a: Set[A]): List[A] =
    a.toList
}
