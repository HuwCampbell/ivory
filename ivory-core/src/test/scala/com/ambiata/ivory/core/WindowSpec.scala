package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class WindowSpec extends Specification with ScalaCheck { def is = s2"""

  Can calculate the date for a window                        $dateLookup
"""

  def dateLookup = prop { (w: Window, d: Date) =>
    Window.startingDate(w)(d) ==== startingDateJoda(w, d)
  }

  def startingDateJoda(window: Window, date: Date): Date = {
    val local = date.localDate
    Date.fromLocalDate((window.unit match {
      case Days   => local.minusDays _
      case Weeks  => local.minusWeeks _
      case Months => local.minusMonths _
      case Years  => local.minusYears _
    })(window.length))
  }
}
