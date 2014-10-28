package com.ambiata.ivory.core
package arbitraries

import org.scalacheck.Arbitrary._
import org.scalacheck._
import ArbitraryValues._

/**
 * Arbitraries for encodings
 */
trait ArbitraryEncodings {

  implicit def EncodingArbitrary: Arbitrary[Encoding] =
    Arbitrary(Gen.oneOf(arbitrary[SubEncoding], arbitrary[ListEncoding]))

  implicit def SubEncodingArbitrary: Arbitrary[SubEncoding] =
    Arbitrary(Gen.oneOf(arbitrary[PrimitiveEncoding], arbitrary[StructEncoding]))

  implicit def PrimitiveEncodingArbitrary: Arbitrary[PrimitiveEncoding] =
    Arbitrary(Gen.oneOf(BooleanEncoding, IntEncoding, LongEncoding, DoubleEncoding, StringEncoding, DateEncoding))

  // TODO needs review
  implicit def StructEncodingArbitrary: Arbitrary[StructEncoding] =
    Arbitrary(Gen.choose(1, 5).flatMap(n => Gen.mapOfN[String, StructEncodedValue](n, for {
      name <- arbitrary[Name].map(_.name)
      enc <- arbitrary[PrimitiveEncoding]
      optional <- arbitrary[Boolean]
    } yield name -> StructEncodedValue(enc, optional)).map(StructEncoding)))

  implicit def ListEncodingArbitrary: Arbitrary[ListEncoding] =
    Arbitrary(arbitrary[SubEncoding].map(ListEncoding))

  implicit def TypeArbitrary: Arbitrary[Type] =
    Arbitrary(Gen.oneOf(NumericalType, ContinuousType, CategoricalType, BinaryType))

  implicit def EncodingAndValueArbitrary: Arbitrary[EncodingAndValue] = Arbitrary(for {
    enc   <- arbitrary[Encoding]
    value <- Gen.frequency(19 -> valueOf(enc, List()), 1 -> Gen.const(TombstoneValue))
  } yield EncodingAndValue(enc, value))

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
    valueOfPrim(encoding).flatMap { v =>
      if (tombstones.contains(Value.toStringPrimitive(v)))
        if (tombstones.nonEmpty) Gen.const(TombstoneValue)
        // There's really nothing we can do here - need to try again
        else valueOfPrimOrTomb(encoding, tombstones)
      else Gen.const(v)
    }

  def valueOfPrim(encoding: PrimitiveEncoding): Gen[PrimitiveValue] = encoding match {
    case BooleanEncoding =>
      arbitrary[Boolean].map(BooleanValue)
    case IntEncoding =>
      arbitrary[Int].map(IntValue)
    case LongEncoding =>
      arbitrary[Long].map(LongValue)
    case DoubleEncoding =>
      arbitrary[Double].retryUntil(Value.validDouble).map(DoubleValue)
    case StringEncoding =>
      // We shouldn't be stripping these out but we need to encode our output first...
      // https://github.com/ambiata/ivory/issues/353
      arbitrary[String].map(_.filter(c => c > 31 && c != '|')).map(StringValue)
    case DateEncoding =>
      arbitrary[Date].map(DateValue)
  }
}

object ArbitraryEncodings extends ArbitraryEncodings

/** value with its corresponding encoding */
case class EncodingAndValue(enc: Encoding, value: Value)
