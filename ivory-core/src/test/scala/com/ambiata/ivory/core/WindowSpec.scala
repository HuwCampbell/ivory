package com.ambiata.ivory.core

import org.specs2._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class WindowSpec extends Specification with ScalaCheck { def is = s2"""

  Can calculate the date for a window                        $dateLookup
  Starting date edge cases                                   $startingDateEdgeCases

"""

  def dateLookup = prop { (w: Window, d: Date) =>
    Window.startingDate(w, d) ==== startingDateJoda(w, d)
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

  // It's a little hard to write general property tests for this
  def startingDateEdgeCases =
    seqToResult(List(
      (Window(1, Days),   Date(2012, 3, 1),  Date(2012, 2, 29), "day: leap year"),
      (Window(1, Weeks),  Date(2012, 3, 7),  Date(2012, 2, 29), "week: leap year"),
      (Window(1, Months), Date(2011, 3, 30), Date(2011, 2, 28), "month: non leap year"),
      (Window(1, Months), Date(2012, 3, 30), Date(2012, 2, 29), "month: leap year"),
      (Window(1, Years),  Date(2012, 2, 29), Date(2011, 2, 28), "year: from leap year"),
      (Window(1, Years),  Date(2013, 2, 28), Date(2012, 2, 28), "year: to leap year"),
      (Window(1, Years),  Date(2014, 2, 28), Date(2013, 2, 28), "year: not leap year")
    ).map {
      case (window, start, e, reason) => (Window.startingDate(window, start) ==== e).setMessage(reason)
    })

}
