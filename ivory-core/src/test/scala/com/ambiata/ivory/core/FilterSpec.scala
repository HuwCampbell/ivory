package com.ambiata.ivory.core

import com.ambiata.ivory.core.Arbitraries._
import org.specs2._

class FilterSpec extends Specification with ScalaCheck { def is = s2"""

  Can serialise to and from a string                       $parse
  Ensure tester is working correctly (for other specs)     $testEval
  Validates encoding of values                             $invalidEncodedValue
  Validates struct names                                   $invalidStructName
  Validates missing struct value                           $missingStructValue
"""

  def parse = prop((f: DefinitionWithFilter) =>
    FilterTextV0.encode(FilterTextV0.asString(f.filter), f.cd.encoding).toEither must beRight(f.filter)
  )

  def testEval = prop((f: FactsWithFilter) =>
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
}

object FilterTester {

  /** Simple but unoptimised version of FilterReducer for specs validation */
  def eval(filter: FilterEncoded, fact: Fact): Boolean =
    filter.fold({
      case FilterEquals(e) => (value: Value) => value == e
    })(
      _(fact.value),
      (name, fe) => fact.value match {
        case StructValue(values) => values.get(name).exists(fe)
        case _                   => false
      }
    )((op, values) => op.fold(values.forall(identity), values.exists(identity)))
}
