package com.ambiata.ivory.core

import org.specs2.matcher.ThrownExpectations
import org.specs2.{ScalaCheck, Specification}
import shapeless.test.illTyped
import Name._

class NameSpec extends Specification with ScalaCheck with ThrownExpectations { def is = s2"""

 A Name is a constrained string which can be used to give a "simple" name to things.

 it contains only characters [A-Z][a-z], numbers [0-9] and underscores '_' or dashes '-' $wellformed
 it must not be empty     $notBeEmpty
 it must not start with _ $notStartWith_

 the compiler can catch if a string literal can be passed to a Name or not $compilationError
"""

  def wellformed = prop { string: String =>
    val nameOption = nameFromString(string)

    nameOption must beSome { name: Name =>
      name.name
        .filterNot(('a' to 'z').contains)
        .filterNot(('A' to 'Z').contains)
        .filterNot(('0' to '0').contains)
        .filterNot(Seq("_", "-").contains) must beEmpty
    }.when(nameOption.isDefined)
  }

  def notBeEmpty = prop { string: String =>
    val nameOption = nameFromString(string)

    nameOption must beSome { name: Name =>
      name.name must not(beEmpty)
    }.when(nameOption.isDefined)
  }

  def notStartWith_ = prop { string: String =>
    val nameOption = nameFromString(string)

    nameOption must beSome { name: Name =>
      name.name must not(startWith("_"))
    }.when(nameOption.isDefined)
  }

  def compilationError = {
    illTyped(""" ("a/b/c": Name) """)
    ok
  }
}
