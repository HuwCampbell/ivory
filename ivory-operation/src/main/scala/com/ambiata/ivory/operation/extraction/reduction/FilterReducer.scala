package com.ambiata.ivory.operation.extraction.reduction

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import scalaz._

class FilterReducer(reduction: Reduction, expression: FilterReductionExpression) extends Reduction {

  def clear(): Unit =
    reduction.clear()

  def update(fact: Fact): Unit =
    if(expression.eval(fact)) {
      reduction.update(fact)
    }

  def save: ThriftFactValue =
    reduction.save
}

object FilterReducer {

  def compile(filter: Filter, encoding: Encoding, reduction: Reduction): String \/ Reduction =
    FilterTextV0.encode(filter, encoding).map(compileEncoded(_, reduction))

  def compileEncoded(filter: FilterEncoded, reduction: Reduction): FilterReducer =
    new FilterReducer(reduction, compileExpression(filter))

  def compileExpression(filter: FilterEncoded): FilterReductionExpression =
    filter match {
      case FilterValues(op, fields) =>
        compileOp(op, fields.map(compilePredicate).map(new FilterValueReducer(_)))
      case FilterStruct(op, fields) => compileOp(op, fields.map {
        case (name, exp) => new FilterStructReducer(name, compilePredicate(exp))
      })
    }

  def compilePredicate(exp: FilterExpression): FilterReducerPredicate =
    exp match {
      case FilterEquals(value) => new FilterReducerEquals(value match {
        case StringValue(v)   => v
        case BooleanValue(v)  => v
        case IntValue(v)      => v
        case LongValue(v)     => v
        case DoubleValue(v)   => v
        case TombstoneValue() => Crash.error(Crash.Invariant, "Impossible code path")
      })
    }

  def compileOp(op: FilterOp, expressions: List[FilterReductionExpression]): FilterReductionExpression =
    op match {
      case FilterOpAnd => new FilterAndReducer(expressions)
      case FilterOpOr  => new FilterOrReducer(expressions)
    }
}

/* Expressions */

trait FilterReductionExpression {
  def eval(fact: Fact): Boolean
}

class FilterAndReducer(expressions: List[FilterReductionExpression]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    // Unrolled call to every() to avoid function allocation
    var these = expressions
    while (!these.isEmpty) {
      if (!these.head.eval(fact)) return false
      these = these.tail
    }
    true
  }
}

class FilterOrReducer(expressions: List[FilterReductionExpression]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    // Unrolled call to exists() to avoid function allocation
    var these = expressions
    while (!these.isEmpty) {
      if (these.head.eval(fact)) return true
      these = these.tail
    }
    false
  }
}

class FilterValueReducer(pred: FilterReducerPredicate) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    // Calling getFieldValue() here which returns 'Object'
    // Unfortunately calling getX() does internal boxing so there isn't anything we can do
    !fact.isTombstone && pred.eval(fact.toThrift.getValue.getFieldValue)
}

class FilterStructReducer(field: String, pred: FilterReducerPredicate) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    if (!fact.isTombstone) {
      val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
      // We may need to pass this through if we add 'is set' as a predicate
      value != null && pred.eval(value.getFieldValue)
    } else false
}

/* Predicates */

trait FilterReducerPredicate {
  def eval(v: Any): Boolean
}

/** We may want to specialize this, although currently `value` in [[ThriftFactPrimitiveValue]] is already boxed */
class FilterReducerEquals(value: Any) extends FilterReducerPredicate {
  def eval(v: Any): Boolean =
    v == value
}
