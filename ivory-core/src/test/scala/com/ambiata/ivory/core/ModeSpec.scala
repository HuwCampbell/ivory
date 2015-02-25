package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.Mode._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class ModeSpec extends Specification with ScalaCheck { def is = s2"""

Combinators
-----------

  State fold only evaluates 'state' expression:

    ${ prop((n: Int) => State.fold(n, ???, _ => ???) ==== n) }

  Set fold only evaluates 'state' expression:

    ${ prop((n: Int) => Set.fold(???, n, _ => ???) ==== n) }

  Set fold only evaluates 'keyed_set' expression:

    ${ prop((n: Int, key: String) => KeyedSet(key).fold(???, ???, _ => + n) ==== n) }

  Fold constructors is identity:

    ${ prop((m: Mode) => m.fold(Mode.state, Mode.set, Mode.keyedSet) ==== m) }

  Render correct strings:

    ${ State.render ==== "state" }

    ${ Set.render ==== "set" }

    ${ prop((key: String) => KeyedSet(key).render ==== "keyed_set," + key) }

  fromString and render are symmetric:

    ${ prop((m: Mode) => Mode.fromString(m.render) ==== Some(m)) }

"""
}
