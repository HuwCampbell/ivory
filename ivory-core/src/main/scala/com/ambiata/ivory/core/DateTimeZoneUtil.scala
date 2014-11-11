package com.ambiata.ivory.core

import org.joda.time.DateTimeZone
import scalaz._, Scalaz._

object DateTimeZoneUtil {

  def forID(id: String): String \/ DateTimeZone =
    try DateTimeZone.forID(id).right
    catch {
      case e: IllegalArgumentException => e.getMessage.left
    }
}
