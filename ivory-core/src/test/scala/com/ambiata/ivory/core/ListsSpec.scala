package com.ambiata.ivory.core

import org.specs2._
import scalaz._

class ListsSpec extends Specification with ScalaCheck { def is = s2"""
Lists Tests
-----------
  findMapM:
    finding the first element performs transform and only runs only necessary effects   $first
    finding the last element performs transform and runs all effects (once only)        $last
    runs all effects but doesn't return a value for not found                           $effects
"""
  import Lists._

  type StateInt[A] = State[Int, A]

  def found(z: Int): State[Int, Option[Int]] =
    State(n => (n + 1, Some(z * 2)))

  def notfound: State[Int, Option[Int]] =
    State(n => (n + 1, None))

  def first = prop((x: Int, xs: List[Int]) =>
    findMapM[StateInt, Int, Int](x :: xs)(found)
      .run(0) must_== (1 -> Some(x * 2)))

  def last = prop((x: Int, xs: List[Int]) => !xs.contains(x) ==> {
    findMapM[StateInt, Int, Int](xs ++ List(x))(z => if (z == x) found(z) else notfound)
      .run(0) must_== ((xs.length + 1) -> Some(x * 2)) })

  def effects = prop((xs: List[Int]) =>
    findMapM[StateInt, Int, Int](xs)(_ => notfound)
      .run(0) must_== (xs.length -> None))
}
