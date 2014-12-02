package com.ambiata.ivory.core

case class Window(length: Int, unit: WindowUnit)

object Window {

  def asString(window: Window): String =
    window.length + " " + ((window.length, window.unit) match {
      case (1, Days)   => "day"
      case (_, Days)   => "days"
      case (1, Weeks)  => "week"
      case (_, Weeks)  => "weeks"
      case (1, Months) => "month"
      case (_, Months) => "months"
      case (1, Years)  => "year"
      case (_, Years)  => "years"
    })

  def unitFromString(unit: String): Option[WindowUnit] =
    unit match {
      case "day"    => Some(Days)
      case "days"   => Some(Days)
      case "week"   => Some(Weeks)
      case "weeks"  => Some(Weeks)
      case "month"  => Some(Months)
      case "months" => Some(Months)
      case "year"   => Some(Years)
      case "years"  => Some(Years)
      case _        => None
    }

  /**
   * Create a lookup function _once_ that knows what to do with a given date.
   * NOTE: We are using our own [[DateTimeUtil]] because we can't run Joda on Hadoop for various reasons,
   * one of which is performance.
   */
  def startingDate(window: Window): Date => Date =
    window.unit match {
      case Days   => d => DateTimeUtil.minusDays(d, window.length)
      case Weeks  => d => DateTimeUtil.minusDays(d, window.length * 7)
      case Months => d => DateTimeUtil.minusMonths(d, window.length)
      case Years  => d => DateTimeUtil.minusYears(d, window.length)
    }

  def isFactWithinWindowRange(windowStartDate: Date, windowEndDate: Date, fact: Fact): Boolean = {
    val date = fact.date
    withinWindow(windowStartDate, date) && date <= windowEndDate
  }

  /** This is preferred over [[withinWindow()]] to avoid any mixup with [[Date]] argument ordering */
  def isFactWithinWindow(windowStartDate: Date, fact: Fact): Boolean =
    withinWindow(windowStartDate, fact.date)

  /**
   * NOTE: This is _exclusive_ - the start date of the window does _not_ count
   * For example we generate the starting date for 8/1, with a 1 week window, as 1/1.
   * Facts on this date would should not be included in the window
   */
  def withinWindow(windowStartDate: Date, factDate: Date): Boolean =
    windowStartDate.underlying < factDate.underlying
}

sealed trait WindowUnit
case object Days extends WindowUnit
case object Weeks extends WindowUnit
case object Months extends WindowUnit
case object Years extends WindowUnit
