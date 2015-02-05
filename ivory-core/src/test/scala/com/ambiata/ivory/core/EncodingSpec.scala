package com.ambiata.ivory.core

import com.ambiata.ivory.core.arbitraries.Arbitraries._
import org.specs2._

class EncodingSpec extends Specification with ScalaCheck { def is = s2"""

Combinators
-----------

  Fold constructors is identity: $foldIdentity

"""

  def foldIdentity = prop { e: Encoding =>
    e.fold(_.toEncoding, _.toEncoding, _.toEncoding) ==== e
  }
}
