package com.ambiata.ivory.core

import org.scalacheck._
import Prop._
import org.specs2.matcher.ThrownExpectations
import org.specs2.{ScalaCheck, Specification}
import shapeless.test.illTyped
import Name._
import Arbitraries._

class NameSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

 A Name is a constrained string which can be used to give a "simple" name to things.

 it contains only characters [A-Z][a-z], numbers [0-9] and underscores '_' or dashes '-' $wellformed
 a valid string must not fail to parse                                                   $valid
 it must not be empty     $notBeEmpty
 it must not start with _ $notStartWith_

 the compiler can catch if a string literal can be passed to a Name or not $compilationError
"""

  def wellformed = forAll(randomStrings) { string: String =>
    val nameOption = nameFromString(string)
    Prop.collect(nameOption.isDefined) {
      nameOption must beSome { name: Name =>
        name.name
          .filterNot(('a' to 'z').contains)
          .filterNot(('A' to 'Z').contains)
          .filterNot(('0' to '9').contains)
          .filterNot(Seq('_', '-').contains) must beEmpty
      }.when(nameOption.isDefined)
    }
  }

  def valid = forAll(validStrings) { string: String =>
    nameFromString(string) aka string must beSome(Name.unsafe(string))
  }

  def notBeEmpty = forAll(randomStrings) { string: String =>
    val nameOption = nameFromString(string)
    Prop.collect(nameOption.isDefined) {
      nameOption must beSome { name: Name =>
        name.name must not(beEmpty)
      }.when(nameOption.isDefined)
    }
  }

  def notStartWith_ = forAll(randomStrings) { string: String =>
    val nameOption = nameFromString(string)
    Prop.collect(string.startsWith("_")) {
      nameOption must beSome { name: Name =>
        name.name must not(startWith("_"))
      }.when(nameOption.isDefined)
    }
  }

  def compilationError = {
    illTyped(""" ("a/b/c": Name) """)
    ok
  }

  def validStrings: Gen[String] =
    GoodNameStringArbitrary.arbitrary.map(_.name)

  def randomStrings: Gen[String] =
    RandomNameStringArbitrary.arbitrary.map(_.name)

  def badStrings: Gen[String] =
    BadNameStringArbitrary.arbitrary.map(_.name)
}
