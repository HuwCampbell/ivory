package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._, Arbitraries._
import org.specs2._

class WindowLookupSpec extends Specification with ScalaCheck { def is = s2"""

  Serialising window to/from an int is symmetrical           $windowAsInt
"""

  def windowAsInt = prop { (window: Option[Window]) =>
    WindowLookup.fromInt(WindowLookup.toInt(window)) ==== window
  }
}
