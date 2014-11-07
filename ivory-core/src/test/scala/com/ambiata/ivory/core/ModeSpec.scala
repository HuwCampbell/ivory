package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.Mode._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class ModeSpec extends Specification with ScalaCheck { def is = s2"""

Combinators
-----------

  State fold only evaluates 'state' expression:

    ${ prop((n: Int) => State.fold(n, ???) ==== n) }

  Set fold only evaluates 'state' expression:

    ${ prop((n: Int) => Set.fold(???, n) ==== n) }

  Fold constructors is identity:

    ${ prop((m: Mode) => m.fold(Mode.state, Mode.set) ==== m) }

  isState is true iff mode is State:

    ${ prop((m: Mode) => m.isState ==== (m == State)) }

  isSet is true iff mode is Set:

    ${ prop((m: Mode) => m.isSet ==== (m == Set)) }

  Render correct strings:

    ${ State.render ==== "state" }

    ${ Set.render ==== "set" }

  fromString and render are symmetric:

    ${ prop((m: Mode) => Mode.fromString(m.render) ==== Some(m)) }

"""
}
