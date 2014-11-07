package com.ambiata.ivory.core.gen

import org.scalacheck.Gen, Gen.Choose


// FIX ARB move to disordert.
object GenPlus {
  // The Gen version of this function results in discarded tests
  def nonEmptyListOf[A](gen: => Gen[A]): Gen[List[A]] =
    Gen.sized(n => Gen.choose(1, Math.max(1, n)).flatMap(i => Gen.listOfN(i, gen)))

  // The Gen version of this function resulted in discarded tests
  def posNum[A](implicit num: Numeric[A], c: Choose[A]): Gen[A] =
    Gen.sized(max => c.choose(num.one, num.max(num.one, num.fromInt(max))))
}
