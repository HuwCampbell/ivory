package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

class NumFlipsReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the num flips of an arbitrary number of booleans           $numFlips
"""

  // Use booleans here to increase the likelihood of duplicates
  def numFlips = prop((xs: List[Option[Boolean]], empty: Boolean) =>
    ReducerUtil.runWithTombstones(new NumFlipsReducer[Boolean](empty), xs) ==== Math.max(compress(xs).length - 1, 0)
  )

  def compress[A](input: List[A]): List[A] = {
    def rec(c: List[A], l: List[A], last: A): List[A] =
      c match {
        case Nil    => l
        case h :: t => rec(t, if (h == last) l else h :: l, h)
      }
    input match {
      case Nil    => Nil
      case h :: t => rec(t, List(h), h).reverse
    }
  }
}
