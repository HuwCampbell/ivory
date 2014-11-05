package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries._
import scalaz._, Scalaz._

class FilterSpec extends Specification with ScalaCheck { def is = s2"""

  Can serialise to and from a string                       $parse
  Ensure tester is working correctly (for other specs)     $testEval
  Validates encoding of values                             $invalidEncodedValue
  Validates struct names                                   $invalidStructName
  Validates missing struct value                           $missingStructValue
  Validates invalid operation                              $invalidOperation
"""

  def parse = prop((f: DefinitionWithQuery) =>
    FilterTextV0.encode(FilterTextV0.asString(f.filter), f.cd.encoding).toEither must beRight(f.filter)
  )

  def testEval = prop((f: FactsWithQuery) =>
    f.facts.forall(FilterTester.eval(f.filter.filter, _)) && !f.other.exists(FilterTester.eval(f.filter.filter, _))
  )

  def invalidEncodedValue = {
    val values = FilterValues(FilterValuesOp(FilterOpAnd, List(FilterEquals(IntValue(1))), Nil))
    FilterTextV0.encode(FilterTextV0.asString(values), BooleanEncoding).toEither must beLeft
  }

  def invalidStructName = {
    val struct = FilterStruct(FilterStructOp(FilterOpAnd, List("a" -> FilterEquals(StringValue("b"))), Nil))
    FilterTextV0.encode(FilterTextV0.asString(struct), StructEncoding(Map())).toEither must beLeft
  }

  def missingStructValue =
    FilterTextV0.encode(Filter("and,a"), StructEncoding(Map())).toEither must beLeft

  def invalidOperation =
    FilterTextV0.encode(Filter("and,(*,x)"), StringEncoding).toEither must beLeft
}

object FilterTester {

  /** Simple but unoptimised version of FilterReducer for specs validation */
  def eval(filter: FilterEncoded, fact: Fact): Boolean =
    filter.fold({
      case FilterEquals(e)             => (value: Value) => value == e
      case FilterLessThan(e)           => compare(e, new Compare { def c[A: Order](x: A, y: A) = x < y })
      case FilterLessThanOrEqual(e)    => compare(e, new Compare { def c[A: Order](x: A, y: A) = x <= y })
      case FilterGreaterThan(e)        => compare(e, new Compare { def c[A: Order](x: A, y: A) = x > y })
      case FilterGreaterThanOrEqual(e) => compare(e, new Compare { def c[A: Order](x: A, y: A) = x >= y })
    })(
      _(fact.value),
      (name, fe) => fact.value match {
        case StructValue(values) => values.get(name).exists(fe)
        case _                   => false
      }
    )((op, values) => op.fold(values.forall(identity), values.exists(identity)))

  trait Compare {
    def c[A : Order](a: A, b: A): Boolean
  }

  def compare[A](a: PrimitiveValue, f: Compare): Value => Boolean = b => (a, b) match {
    case (StringValue(x), StringValue(y))     => f.c(y, x)
    case (StringValue(_), _)                  => false
    case (IntValue(x)   , IntValue(y))        => f.c(y, x)
    case (IntValue(_)   , _)                  => false
    case (DoubleValue(x), DoubleValue(y))     => f.c(y, x)
    case (DoubleValue(_), _)                  => false
    case (LongValue(x)  , LongValue(y))       => f.c(y, x)
    case (LongValue(_)  , _)                  => false
    case (BooleanValue(x)  , BooleanValue(y)) => f.c(y, x)
    case (BooleanValue(_)  , _)               => false
    case (DateValue(x)  , DateValue(y))       => f.c(y, x)
    case (DateValue(_)  , _)                  => false
  }
}
