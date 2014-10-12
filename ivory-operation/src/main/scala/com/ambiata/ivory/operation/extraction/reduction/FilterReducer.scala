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
    new FilterReductionIgnoreTombstone(filter.fold(identity)({
      case FilterEquals(value) => value match {
        case StringValue(v)   => new FilterValueReducer(new FilterReducerEquals(v), ReductionValueString)
        case BooleanValue(v)  => new FilterValueReducer(new FilterReducerEquals(v), ReductionValueBoolean)
        case IntValue(v)      => new FilterValueReducer(new FilterReducerEquals(v), ReductionValueInt)
        case LongValue(v)     => new FilterValueReducer(new FilterReducerEquals(v), ReductionValueLong)
        case DoubleValue(v)   => new FilterValueReducer(new FilterReducerEquals(v), ReductionValueDouble)
        case DateValue(v)     => new FilterValueReducer(new FilterReducerEquals(v.int), ReductionValueDate)
      }
    }, {
      (name, exp) => exp match {
        case FilterEquals(value) => value match {
          case StringValue(v)   => new FilterStructReducer(name, new FilterReducerEquals(v), ReductionValueString)
          case BooleanValue(v)  => new FilterStructReducer(name, new FilterReducerEquals(v), ReductionValueBoolean)
          case IntValue(v)      => new FilterStructReducer(name, new FilterReducerEquals(v), ReductionValueInt)
          case LongValue(v)     => new FilterStructReducer(name, new FilterReducerEquals(v), ReductionValueLong)
          case DoubleValue(v)   => new FilterStructReducer(name, new FilterReducerEquals(v), ReductionValueDouble)
          case DateValue(v)     => new FilterStructReducer(name, new FilterReducerEquals(v.int), ReductionValueDate)
        }
      }
    }) {
      (op, expressions) => op match {
        case FilterOpAnd => new FilterAndReducer(expressions)
        case FilterOpOr  => new FilterOrReducer(expressions)
      }
    })
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

/** This may need to be removed once we support more than equals */
class FilterReductionIgnoreTombstone(p: FilterReductionExpression) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    !fact.isTombstone && p.eval(fact)
}

/* Values */

class FilterValueReducer[A](pred: FilterReducerPredicate[A], from: ReductionValueFrom[A]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(from.from(fact.toThrift.getValue))
}

class FilterValueReducerDate(pred: FilterReducerPredicate[Date]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(Date.unsafeFromInt(fact.toThrift.getValue.getDate))
}

/* Structs */

class FilterStructReducer[A](field: String, pred: FilterReducerPredicate[A], from: ReductionValueFromPrim[A]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && pred.eval(from.fromPrim(value))
  }
}

/* Predicates */

trait FilterReducerPredicate[@specialized(Boolean, Int, Long, Double) A] {
  def eval(v: A): Boolean
}

class FilterReducerEquals[A](a: A) extends FilterReducerPredicate[A] {
  def eval(v: A): Boolean = a == v
}
