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

}

sealed trait WindowUnit
case object Days extends WindowUnit
case object Weeks extends WindowUnit
case object Months extends WindowUnit
case object Years extends WindowUnit
