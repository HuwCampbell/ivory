package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._

import scalaz._, Scalaz._

class FormatSpec extends Specification with ScalaCheck { def is = s2"""

Combinators
-----------

  fromString/render symmetry:                            $string

"""

  def string = prop ((f: FileFormat, ns: Option[Namespace], path: String) => f.fold((_, _, j) => j.fold(false, true), true) ==> {
    InputFormat.fromString(InputFormat.render(f, ns, path)) ==== (f, ns, path).right
  })
}
