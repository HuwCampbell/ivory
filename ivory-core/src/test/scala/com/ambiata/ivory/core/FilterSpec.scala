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
    val values = FilterValues(FilterOpAnd, List(FilterEquals(IntValue(1))))
    FilterTextV0.encode(FilterTextV0.asString(values), BooleanEncoding).toEither must beLeft
  }

  def invalidStructName = {
    val struct = FilterStruct(FilterOpAnd, List("a" -> FilterEquals(StringValue("b"))))
    FilterTextV0.encode(FilterTextV0.asString(struct), StructEncoding(Map())).toEither must beLeft
  }

  def missingStructValue =
    FilterTextV0.encode(Filter("and,a"), StructEncoding(Map())).toEither must beLeft
}

object FilterTester {

  /** Simple but unoptimised version of FilterReducer for specs validation */
  def eval(filter: FilterEncoded, fact: Fact): Boolean = {
    def evalValue(exp: FilterExpression, value: Value): Boolean = exp match {
      case FilterEquals(e) => value == e
    }
    (filter match {
      case FilterValues(op, fields) => op -> fields.map(_ -> fact.value)
      case FilterStruct(op, fields) => op -> {
        fact.value match {
          case StructValue(values) => fields.flatMap(f => values.get(f._1).map(f._2 ->))
          case _                   => Nil
        }
      }
    }) match {
      case (FilterOpAnd, fields) => fields.forall((evalValue _).tupled)
      case (FilterOpOr, fields)  => fields.exists((evalValue _).tupled)
    }
  }
}
