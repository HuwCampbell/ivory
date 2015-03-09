package com.ambiata.ivory.core

import com.ambiata.ivory.core.gen._
import org.scalacheck._

case class FactsWithKeyedSetPriority(keys: List[(String, PrimitiveEncoding, List[PrimitiveValue])],
                                     datetimes: List[DateTime], encoding: StructEncoding, struct: StructValue) {

  // Because we're a struct we can always append an extra offset value which ensures uniqueness across priority
  val keyOffset = "_offset_"

  def definition(cd: ConcreteDefinition): ConcreteDefinition =
    cd.copy(encoding = StructEncoding(encoding.values ++
      keys.map(x => x._1 -> StructEncodedValue.mandatory(x._2)).toMap +
      (keyOffset -> StructEncodedValue.mandatory(IntEncoding))
    ).toEncoding, mode = Mode.KeyedSet(keys.map(_._1)))

  def factsets(template: Fact): List[List[Fact]] = {
    val max = keys.map(_._3.size).max
    (0 until max).map(i => {
      val values = keys.flatMap(x => x._3.drop(i).headOption.orElse(x._3.headOption).map(x._1 -> _))
      datetimes.map(dt =>
        template
          .withValue(StructValue(struct.values ++ values.toMap ++ Map(keyOffset -> IntValue(i))))
          .withDateTime(dt)
      )
    }).toList.reverse
  }

  def dates: List[Date] =
    datetimes.map(_.date)
}

object FactsWithKeyedSetPriority {

  implicit def FactsWithKeyedSetPriorityArbitrary: Arbitrary[FactsWithKeyedSetPriority] =
    Arbitrary(for {
      k <- GenPlus.listOfSizedWithIndex(1, 3, i => for {
        k <- GenString.sensible.map(s => s"k_${s}_${i}")
        p <- GenDictionary.primitiveEncoding
        // FIX Increase the max size here once snapshot and squash are more aligned
        v <- GenPlus.listOfSized(1, 1, GenValue.valueOfPrim(p))
      } yield (k, p, v))
      d <- GenPlus.listOfSized(1, 10, GenDate.dateTime)
      // We use state here because we're going to add our own key value anyway
      e <- GenDictionary.structEncodingWithMode(Mode.State)
      s <- GenValue.valueOfStruct(e)
    } yield FactsWithKeyedSetPriority(k, d, e, s))
}
