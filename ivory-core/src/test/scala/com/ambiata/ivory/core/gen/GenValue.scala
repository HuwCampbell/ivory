package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._
import org.scalacheck._, Arbitrary._


object GenValue {
  def primitiveValue: Gen[PrimitiveValue] =
    Gen.frequency(
        1 -> (Gen.identifier map StringValue.apply)
      , 2 -> (arbitrary[Int] map IntValue.apply)
      , 2 -> (arbitrary[Long] map LongValue.apply)
      , 2 -> (arbitrary[Double] map DoubleValue.apply)
      , 2 -> (arbitrary[Boolean] map BooleanValue.apply)
      , 2 -> (GenDate.date map DateValue.apply)
      )

  def valueOf(encoding: Encoding, tombstones: List[String]): Gen[Value] = encoding match {
    case p: PrimitiveEncoding => valueOfPrimOrTomb(p, tombstones)
    case sub: SubEncoding => valueOfSub(sub)
    case ListEncoding(sub) => Gen.listOf(valueOfSub(sub)).map(ListValue)
  }

  def valueOfSub(encoding: SubEncoding): Gen[SubValue] = encoding match {
    case p: PrimitiveEncoding => valueOfPrim(p)
    case StructEncoding(s) =>
      Gen.sequence[Seq, Option[(String, PrimitiveValue)]](s.map { case (k, v) =>
        for {
          p <- valueOfPrim(v.encoding).map(k ->)
          // _Sometimes_ generate a value for optional fields :)
          b <- if (v.optional) arbitrary[Boolean] else Gen.const(true)
        } yield if (b) Some(p) else None
      }).map(_.flatten.toMap).map(StructValue)
  }

  def valueOfPrimOrTomb(encoding: PrimitiveEncoding, tombstones: List[String]): Gen[Value] =
    valueOfPrim(encoding).flatMap(v =>
      if (tombstones.contains(Value.toStringPrimitive(v)))
        if (tombstones.nonEmpty) Gen.const(TombstoneValue)
        // There's really nothing we can do here - need to try again
        else valueOfPrimOrTomb(encoding, tombstones)
      else Gen.const(v))

  def valueOfPrim(encoding: PrimitiveEncoding): Gen[PrimitiveValue] = encoding match {
    case BooleanEncoding =>
      arbitrary[Boolean].map(BooleanValue)
    case IntEncoding =>
      arbitrary[Int].map(IntValue)
    case LongEncoding =>
      arbitrary[Long].map(LongValue)
    case DoubleEncoding =>
      // FIX ARB avoid retry.
      arbitrary[Double].retryUntil(Value.validDouble).map(DoubleValue)
    case StringEncoding =>
      // We shouldn't be stripping these out but we need to encode our output first...
      // https://github.com/ambiata/ivory/issues/353
      Gen.frequency(
        2 -> arbitrary[String].map(_.filter(c => c > 31 && c != '|'))
      , 8 -> Gen.identifier
      ).map(StringValue)

    case DateEncoding =>
      GenDate.date.map(DateValue)
  }
}
