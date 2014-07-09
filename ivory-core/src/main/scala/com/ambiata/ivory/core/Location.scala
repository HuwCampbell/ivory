package com.ambiata.ivory.core

import com.ambiata.mundane.io.{Location => MLocation, _}
import scalaz._, Scalaz._

object Location {
  def fromUri(s: String): String \/ MLocation = try {
    val uri = new java.net.URI(s)
    uri.getScheme match {
      case null =>
        // TODO Should be LocalLocation but our own consumers aren't ready yet
        // https://github.com/ambiata/ivory/issues/87
        HdfsLocation(uri.getPath).right
      case _ => MLocation.fromUri(s)
    }
  } catch {
    case e: java.net.URISyntaxException =>
      e.getMessage.left
  }
}
