package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import org.specs2.{ScalaCheck, Specification}
import com.ambiata.ivory.core.thrift.ThriftFactValue


class InverseReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Inverse count reducer works with doubles       $inverseDouble
  Inverse count reducer works with ints          $inverseInt

"""

  def inverseDouble = prop((number: Double) => {
    val r = new InverseReducer(new DummyReducer[Double](number, ReductionValueDouble))
    r.save ==== ThriftFactValue.d(if (number == 0.0) Double.NaN else 1.0 / number)
  })

  def inverseInt = prop((number: Int) => {
    val r = new InverseReducer(new DummyReducer[Int](number, ReductionValueInt))
    r.save ==== ThriftFactValue.d(if (number == 0) Double.NaN else 1.0 / number)
  })

  class DummyReducer[A](ret: A, v: ReductionValueTo[A]) extends Reduction {
    val value = new ThriftFactValue()
    def clear(): Unit = ()
    def update(f: Fact): Unit = ()
    def skip(f: Fact, reason: String): Unit = ()
    def save: ThriftFactValue = {
      v.to(ret, value)
      value
    }
  }

}
