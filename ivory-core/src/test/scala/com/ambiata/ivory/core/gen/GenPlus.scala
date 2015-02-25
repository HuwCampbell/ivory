package com.ambiata.ivory.core.gen

import org.scalacheck.Gen, Gen.Choose
import scalaz._, Scalaz._, scalaz.scalacheck.ScalaCheckBinding._

// FIX ARB move to disorder.
object GenPlus {
  // The Gen version of this function results in discarded tests
  def nonEmptyListOf[A](gen: => Gen[A]): Gen[List[A]] =
    Gen.sized(n => Gen.choose(1, Math.max(1, n)).flatMap(i => Gen.listOfN(i, gen)))

  // The Gen version of this function resulted in discarded tests
  def posNum[A](implicit num: Numeric[A], c: Choose[A]): Gen[A] =
    Gen.sized(max => c.choose(num.one, num.max(num.one, num.fromInt(max))))

  def sizedInt(from: Int, to: Int): Gen[Int] =
    Gen.sized(n => Gen.choose(from, Math.max(from, Math.min(n, to))))

  def listOfSized[A](from: Int, to: Int, gen: => Gen[A]): Gen[List[A]] =
    sizedInt(from, to).flatMap(i => Gen.listOfN(i, gen))

  /** Useful for tagging a Gen value with a unique int */
  def listOfSizedWithIndex[A](from: Int, to: Int, gen: Int => Gen[A]): Gen[List[A]] =
    sizedInt(from, to).flatMap(i => (0 until i).toList.traverse(gen))
}
