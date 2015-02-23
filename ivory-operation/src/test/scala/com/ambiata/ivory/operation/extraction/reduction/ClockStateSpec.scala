package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.disorder.NaturalIntSmall
import org.specs2.{ScalaCheck, Specification}
import org.specs2.execute.Result

class ClockStateSpec extends Specification with ScalaCheck { def is = s2"""

  The unwound list, should always be the length of the backing array, unless
  there are not enough input values to fill clock, and then it will have a
  length the same as the input list:

    ${ check((state, n, l) => state.unwind.size ==== math.min(l.size, n)) }


  Count should always be same as number of tick calls:

    ${ check((state, n, l) => state.count ==== l.size) }


  Hand should always be same as number of tick calls mod the length of the
  backing array:

    ${ check((state, n, l) => state.hand ==== (l.size % n)) }


  The final result should equal, the original list, take the last n elements,
  or the whole list, if the length of the list is less than n:

    ${ check((state, n, l) => state.unwind ==== l.takeRight(math.min(n, l.size)).reverse) }


  Unwind always returns the same result until more values or reset:

    ${ check((state, n, l) => state.unwind ==== state.unwind) }


  Resetting and propelling produces same result:

    ${ check((state, n, l) => {
      val first = state.unwind
      val again = { state.reset; l.foreach(state.tick); state.unwind }
      again ==== first }) }


  Resetting and propelling after different ordering produces same result:

    ${ check((state, n, l) => {
      val first = state.unwind
      state.reset; l.reverse.foreach(state.tick); state.unwind
      val again = { state.reset; l.foreach(state.tick); state.unwind }
      again ==== first }) }

"""

  def check(pred: (ClockState[Int], Int, List[Int]) => Result) =
    propNoShrink((n: NaturalIntSmall, l: List[Int]) =>  pred(propel(n.value, l), n.value, l))

  def propel(n: Int, l: List[Int]): ClockState[Int] = {
    val state = ClockState(new Array[Int](n))
    l.foreach(state.tick)
    state
  }
}
