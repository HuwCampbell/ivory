package com.ambiata.ivory.storage.lookup

import com.ambiata.ivory.core._

/** Use an int to represent the Window, where the first byte represents the unit */
object WindowLookup {

  def toInt(window: Option[Window]): Int =
    window.map { w =>
      ((w.unit match {
        case Days   => 0
        case Weeks  => 1
        case Months => 2
        case Years  => 3
      }) << 30) | (w.length & 0x7FFF)
    }.getOrElse(0)

  def fromInt(w: Int): Option[Window] =
    if (w == 0) None
    else Some(Window(w & 0x7FFF, w >>> 30 match {
      case 0 => Days
      case 1 => Weeks
      case 2 => Months
      case 3 => Years
    }))
}
