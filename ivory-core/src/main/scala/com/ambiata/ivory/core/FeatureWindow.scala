package com.ambiata.ivory.core

import scalaz._

case class FeatureWindow(id: FeatureId, windows: List[Window])

object FeatureWindow {
  implicit def FeatureWindowEqual: Equal[FeatureWindow] =
    Equal.equalA
}
