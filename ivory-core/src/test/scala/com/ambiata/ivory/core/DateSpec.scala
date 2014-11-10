package com.ambiata.ivory.core

import org.specs2._
import scalaz._, Scalaz._
import com.ambiata.ivory.core.arbitraries._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

class DateSpec extends Specification with ScalaCheck { def is = s2"""

Field Accessors
---------------

  Year, month and day reflect actual values:

    ${ prop((y: Year) => Date.create(y.year, 1, 1).map(_.year) ==== y.year.some) }

    ${ prop((m: Month) => Date.create(2010, m.month, 1).map(_.month) ==== m.month.some) }

    ${ prop((d: Day) => Date.create(2010, 1, d.day).map(_.day) ==== d.day.some) }

Combinators
-----------

  Add time is consistent for date and time values:

    ${ prop((d: Date, t: Time) => d.addTime(t).time ==== t) }

    ${ prop((d: Date, t: Time) => d.addTime(t).date ==== d) }


  Symmetric with joda local date:

    ${ prop((d: Date) => {
         val l = d.localDate
         Date.create(
           l.getYear.toShort
         , l.getMonthOfYear.toByte
         , l.getDayOfMonth.toByte) ==== d.some }) }

  Symmetric int construction:

    ${ prop((d: Date) => Date.fromInt(d.int) ==== d.some) }


Rendering Dates
---------------

  Render hyphenated has the expected form and values, being `yyyy-MM-dd`:

    ${ prop((d: Date) => d.hyphenated.length ==== 10) }

    ${ prop((d: Date) => d.hyphenated.charAt(4) ==== '-') }

    ${ prop((d: Date) => d.hyphenated.charAt(7) ==== '-') }

    ${ prop((d: Date) =>
         d.hyphenated.substring(0, 4).parseInt.toOption.map(_.toShort) ==== d.year.some) }

    ${ prop((d: Date) =>
         d.hyphenated.substring(5, 7).parseInt.toOption.map(_.toByte) ==== d.month.some) }

    ${ prop((d: Date) =>
         d.hyphenated.substring(8, 10).parseInt.toOption.map(_.toByte) ==== d.day.some) }


  Render slashed is identical to hyphenated except for separator:

    ${ prop((d: Date) => d.slashed ==== d.hyphenated.replace('-', '/')) }


Comparing Dates
---------------

  Comparisons on the whole date should be the same as comparing components:

    ${ prop((a: Date, b: Date) => (a < b) ==== (a.components < b.components)) }

    ${ prop((a: Date, b: Date) => (a <= b) ==== (a.components <= b.components)) }

    ${ prop((a: Date, b: Date) => (a > b) ==== (a.components > b.components)) }

    ${ prop((a: Date, b: Date) => (a >= b) ==== (a.components >= b.components)) }


  Aliases should be consistent:

    ${ prop((a: Date, b: Date) => (a < b) ==== a.isBefore(b)) }

    ${ prop((a: Date, b: Date) => (a <= b) ==== a.isBeforeOrEqual(b)) }

    ${ prop((a: Date, b: Date) => (a > b) ==== a.isAfter(b)) }

    ${ prop((a: Date, b: Date) => (a >= b) ==== a.isAfterOrEqual(b)) }

"""
}
