package com.ambiata.ivory.mr

import org.specs2._

class MapStringSpec extends Specification with ScalaCheck { def is = s2"""

  fromString/render symmetry:                             $renderAsString

"""

  def renderAsString = prop((l: List[(String, String)]) => {
    val m = l.filterNot(x => x._1.contains(MapString.sep1) || x._2.contains(MapString.sep1) || x._1.contains(MapString.sep2)).toMap
    MapString.fromString(MapString.render(m)) ==== m
  })
}
