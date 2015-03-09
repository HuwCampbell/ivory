package com.ambiata.ivory.mr

import org.scalacheck._, Arbitrary._
import org.specs2._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class MutableOptionSpec extends Specification with ScalaCheck { def is = s2"""


Laws
----

  Equal                                        ${equal.laws[MutableOption[Int]]}

Properties
----------

A none option is never set
  $isSetNone

A some value is always set
  $isSetSome

Getting from none throws an exception
  $getNone

Getting from some returns the value
  $getSome

Setting a new value always returns the value
  $set

Can unset the value
  $setNone

toOption and fromOption are symmetrical
  $toOption

equals is the same as Equal
  $equals
"""

  def isSetNone = prop((s: Int) =>
    MutableOption.none(s).isSet ==== false
  )

  def isSetSome = prop((s: Int) =>
    MutableOption.some(s).isSet ==== true
  )

  def getNone = prop((s: Int) =>
    MutableOption.none(s).get must throwA[RuntimeException]
  )

  def getSome = prop((s: Int) =>
    MutableOption.some(s).get ==== s
  )

  def set = prop((o: MutableOption[Int], s: Int) =>
    MutableOption.set(o, s).get ==== s
  )

  def setNone = prop((o: MutableOption[Int]) =>
    MutableOption.setNone(o).isSet ==== false
  )

  def toOption = prop((o: MutableOption[Int], i: Int) =>
    MutableOption.fromOption(o.toOption, i) ==== o
  )

  def equals = prop((i1: MutableOption[Boolean], i2: MutableOption[Boolean]) =>
    implicitly[Equal[MutableOption[Boolean]]].equal(i1, i2) ==== i1.equals(i2)
  )

  implicit def MutableOptionArbitrary[A: Arbitrary]: Arbitrary[MutableOption[A]] =
    Arbitrary(Gen.zip(arbitrary[Boolean], arbitrary[A]).map(x =>
      if (x._1) MutableOption.some(x._2) else MutableOption.none(x._2)
    ))
}
