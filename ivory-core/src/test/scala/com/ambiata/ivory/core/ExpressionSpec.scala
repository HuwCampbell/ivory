package com.ambiata.ivory.core

import com.ambiata.ivory.core.StructEncodedValue.mandatory
import org.specs2._
import com.ambiata.ivory.core.arbitraries._

class ExpressionSpec extends Specification with ScalaCheck { def is = s2"""

  Can serialise to and from a string                       $parse
  Can fail on parsing with bad expression input            $parseFail
  Will validate any correct expression + encoding          $validation
  Can fail on validation with invalid encoding             $validationFail
"""

  def parse = prop((d: DefinitionWithQuery) =>
    Expression.parse(Expression.asString(d.expression)).toEither must beRight(d.expression)
  )

  def parseFail = seqToResult(List(
    "quantile_in_days,3,s",
    "quantile_in_weeks,s,3",
    "proportion_by_time,24,3",
    "interval,noexpression",
    "inverse,countby,things"
  ).map(s => Expression.parse(s).toEither must beLeft))


  def validation = prop((d: DefinitionWithQuery) =>
    Expression.validate(d.expression, d.cd.encoding).toEither must beRight
  )

  def validationFail = seqToResult(List(
    BasicExpression(Sum)               -> StringEncoding,
    BasicExpression(CountUnique)       -> IntEncoding,
    Interval(NumFlips)                 -> LongEncoding,
    StructExpression("a", CountUnique) -> StructEncoding(Map("b" -> mandatory(StringEncoding))),
    StructExpression("a", Mean)        -> StructEncoding(Map("a" -> mandatory(StringEncoding))),
    SumBy("k", "v")                    -> StringEncoding,
    SumBy("k", "v")                    -> StructEncoding(Map("k" -> mandatory(IntEncoding), "v" -> mandatory(IntEncoding))),
    SumBy("k", "v")                    -> StructEncoding(Map("k" -> mandatory(StringEncoding), "v" -> mandatory(BooleanEncoding))),
    CountBySecondary("k", "v")         -> StringEncoding,
    CountBySecondary("k", "v")         -> StructEncoding(Map("k" -> mandatory(StringEncoding)))
  ).map((Expression.validate _).tupled).map(_.toEither must beLeft))
}
