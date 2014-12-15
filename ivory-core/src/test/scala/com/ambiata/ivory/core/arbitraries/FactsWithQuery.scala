package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._


/** list of facts with a query and the corresponding result */
case class FactsWithQuery(filter: DefinitionWithQuery, facts: List[Fact], other: List[Fact])

object FactsWithQuery {
  // FIX ARB Could probably do with some polish / splitting.
  implicit def FactsWithQueryArbitrary: Arbitrary[FactsWithQuery] = Arbitrary(for {
    d <- arbitrary[DefinitionWithQuery]
    n <- Gen.choose(0, 10)
    e <- GenEntity.entity
    f <- Gen.listOfN(n, for {
      f <- GenFact.factWithZone(Gen.const(e), Gen.const(d.cd))
      // This is probably going to get pretty hairy when we add more filter operations
      v = d.filter.fold({
        case FilterEquals(ev) => ev
        case FilterNotEquals(ev) => ev match {
          case StringValue(vv)  => StringValue(vv + "!")
          case IntValue(vv)     => IntValue(vv - 1)
          case LongValue(vv)    => LongValue(vv - 1)
          case DoubleValue(vv)  => DoubleValue(vv - 1)
          case BooleanValue(vv) => BooleanValue(!vv)
          case DateValue(vv)    => DateValue(Date.fromLocalDate(vv.localDate.minusDays(1)))
        }
        case FilterLessThan(ev) => ev match {
          case StringValue(vv)  => StringValue((vv.charAt(0) - 1).toChar.toString ++ vv.substring(1))
          case IntValue(vv)     => IntValue(vv - 1)
          case LongValue(vv)    => LongValue(vv - 1)
          case DoubleValue(vv)  => DoubleValue(Double.MinValue)
          case BooleanValue(vv) => BooleanValue(vv)
          case DateValue(vv)    => DateValue(Date.fromLocalDate(vv.localDate.minusDays(1)))
        }
        case FilterLessThanOrEqual(ev) => ev
        case FilterGreaterThan(ev) => ev match {
          case StringValue(vv)  => StringValue(vv + "a")
          case IntValue(vv)     => IntValue(vv + 1)
          case LongValue(vv)    => LongValue(vv + 1)
          case DoubleValue(vv)  => DoubleValue(Double.MaxValue)
          case BooleanValue(vv) => BooleanValue(vv)
          case DateValue(vv)    => DateValue(Date.fromLocalDate(vv.localDate.plusDays(1)))
        }
        case FilterGreaterThanOrEqual(ev) => ev
      })(identity, (k, v) => StructValue(Map(k -> v))) {
        case (op, h :: t) => op.fold(t.foldLeft(h) {
          case (StructValue(m1), StructValue(m2)) => StructValue(m1 ++ m2)
          case (a, _) => a
        }, h)
        case (_, Nil) => StringValue("")
      }
    } yield f._2.withValue(v))
    o <- Gen.choose(0, 10).flatMap(n => Gen.listOfN(n, GenFact.factWithZone(Gen.const(e), Gen.const(d.cd)).map(_._2)).map(_.filterNot {
      // Filter out facts that would otherwise match
      fact => FilterTester.eval(d.filter, fact)
    }))
  } yield FactsWithQuery(d, f, o))
}
