package com.ambiata.ivory.core

import com.ambiata.disorder._
import com.ambiata.ivory.core.gen.{GenPlus => _, _}
import org.scalacheck._

case class FactsWithKeyedSetPriority(key: String, keyEncoding: PrimitiveEncoding, encoding: StructEncoding,
                                     struct: StructValue, values: Map[PrimitiveValue, Map[DateTime, Int]]) {

  // Because we're a struct we can always append an extra offset value which ensures uniqueness across priority
  val keyOffset = "_offset_"

  def definition(cd: ConcreteDefinition): ConcreteDefinition =
    cd.copy(encoding = StructEncoding(encoding.values +
      (key -> StructEncodedValue.mandatory(keyEncoding)) +
      (keyOffset -> StructEncodedValue.mandatory(IntEncoding))
    ).toEncoding, mode = Mode.KeyedSet(key))

  def factsets(template: Fact): List[List[Fact]] = {
    val max = values.values.flatMap(_.values).max
    (0 until max)
      .map(i => values
        .flatMap(x => x._2.map(y => (x._1, y._1) -> y._2))
        .filter(_._2 > i)
        .map(x => (x._1._1, x._1._2, i)).toList
      ).toList
      .map(_.map(x => {
        template
          .withValue(StructValue(struct.values ++ Map(key -> x._1, keyOffset -> IntValue(x._3))))
          .withDateTime(x._2)
      })).reverse
  }

  def dates: List[Date] =
    values.flatMap(_._2.keys).map(_.date).toList
}

object FactsWithKeyedSetPriority {

  implicit def FactsWithKeyedSetPriorityArbitrary: Arbitrary[FactsWithKeyedSetPriority] =
    Arbitrary(for {
      p <- GenDictionary.primitiveEncoding
      l <- GenPlus.listOfSized(2, 10, for {
        v <- GenValue.valueOfPrim(p)
        n <- GenPlus.listOfSized(1, 3, Gen.zip(
          GenDate.dateTime,
          Arbitrary.arbitrary[NaturalIntSmall].map(_.value)
        )).map(_.toMap)
      } yield v -> n)
      k <- GenString.sensible
      // We use state here because we're going to add our own key value anyway
      e <- GenDictionary.structEncodingWithMode(Mode.State)
      s <- GenValue.valueOfStruct(e)
    } yield FactsWithKeyedSetPriority(k, p, e, s, l.toMap))
}
