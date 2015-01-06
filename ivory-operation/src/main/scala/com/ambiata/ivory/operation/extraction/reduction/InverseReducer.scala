package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core.{Fact, Crash}

class InverseReducer(r: Reduction) extends Reduction {

  val value = new ThriftFactValue()
  val tombstone = new ThriftTombstone

  def clear(): Unit = {
    r.clear()
  }

  def update(fact: Fact): Unit = {
    r.update(fact)
  }

  def skip(f: Fact, reason: String): Unit = r.skip(f, reason)

  def save: ThriftFactValue = {
    r.save  match {
      case x if x.isSetT => value.setT(tombstone)
      case x if x.isSetD =>
        val y = x.getD
        if (y == 0.0) value.setD(Double.NaN) else value.setD(1.0 / y)
      case x if x.isSetI =>
        val y = x.getI
        if (y == 0) value.setD(Double.NaN) else value.setD(1.0 / y.toDouble)
      case x if x.isSetL =>
        val y = x.getL
        if (y == 0L) value.setD(Double.NaN) else value.setD(1.0 / y.toDouble)
      case _ => Crash.error(Crash.CodeGeneration, s"You have hit an expression error as a non numeric fact has been passed into the inverse reducer. This is a BUG.")
    }
    value
  }
}
