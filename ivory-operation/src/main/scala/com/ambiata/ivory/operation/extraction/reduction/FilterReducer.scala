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
        case StringValue(v)   => new FilterValueReducerString(new FilterReducerEquals(v))
        case BooleanValue(v)  => new FilterValueReducerBoolean(new FilterReducerEquals(v))
        case IntValue(v)      => new FilterValueReducerInt(new FilterReducerEquals(v))
        case LongValue(v)     => new FilterValueReducerLong(new FilterReducerEquals(v))
        case DoubleValue(v)   => new FilterValueReducerDouble(new FilterReducerEquals(v))
        case DateValue(v)     => new FilterValueReducerDate(new FilterReducerEquals(v))
      }
    }, {
      (name, exp) => exp match {
        case FilterEquals(value) => value match {
          case StringValue(v)   => new FilterStructReducerString(name, new FilterReducerEquals(v))
          case BooleanValue(v)  => new FilterStructReducerBoolean(name, new FilterReducerEquals(v))
          case IntValue(v)      => new FilterStructReducerInt(name, new FilterReducerEquals(v))
          case LongValue(v)     => new FilterStructReducerLong(name, new FilterReducerEquals(v))
          case DoubleValue(v)   => new FilterStructReducerDouble(name, new FilterReducerEquals(v))
          case DateValue(v)     => new FilterStructReducerDate(name, new FilterReducerEquals(v))
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

class FilterValueReducerString(pred: FilterReducerPredicate[String]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(fact.toThrift.getValue.getS)
}

class FilterValueReducerBoolean(pred: FilterReducerPredicate[Boolean]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(fact.toThrift.getValue.getB)
}

class FilterValueReducerInt(pred: FilterReducerPredicate[Int]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(fact.toThrift.getValue.getI)
}

class FilterValueReducerLong(pred: FilterReducerPredicate[Long]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(fact.toThrift.getValue.getL)
}

class FilterValueReducerDouble(pred: FilterReducerPredicate[Double]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(fact.toThrift.getValue.getD)
}

class FilterValueReducerDate(pred: FilterReducerPredicate[Date]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean =
    pred.eval(Date.unsafeFromInt(fact.toThrift.getValue.getDate))
}

/* Structs */

class FilterStructReducerString(field: String, pred: FilterReducerPredicate[String]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && value.isSetS && pred.eval(value.getS)
  }
}

class FilterStructReducerBoolean(field: String, pred: FilterReducerPredicate[Boolean]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && value.isSetB && pred.eval(value.getB)
  }
}

class FilterStructReducerInt(field: String, pred: FilterReducerPredicate[Int]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && value.isSetI && pred.eval(value.getI)
  }
}

class FilterStructReducerLong(field: String, pred: FilterReducerPredicate[Long]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && value.isSetL && pred.eval(value.getL)
  }
}

class FilterStructReducerDouble(field: String, pred: FilterReducerPredicate[Double]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && value.isSetD && pred.eval(value.getD)
  }
}

class FilterStructReducerDate(field: String, pred: FilterReducerPredicate[Date]) extends FilterReductionExpression {
  def eval(fact: Fact): Boolean = {
    val value = fact.toThrift.getValue.getStructSparse.getV.get(field)
    value != null && value.isSetDate && pred.eval(Date.unsafeFromInt(value.getDate))
  }
}
/* Predicates */

trait FilterReducerPredicate[@specialized(Boolean, Int, Long, Double) A] {
  def eval(v: A): Boolean
}

class FilterReducerEquals[A](a: A) extends FilterReducerPredicate[A] {
  def eval(v: A): Boolean = a == v
}
