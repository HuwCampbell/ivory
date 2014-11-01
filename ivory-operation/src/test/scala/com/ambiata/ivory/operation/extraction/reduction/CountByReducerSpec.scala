package com.ambiata.ivory.operation.extraction.reduction

import org.specs2.{ScalaCheck, Specification}

import scala.collection.JavaConverters._

class CountByReducerSpec extends Specification with ScalaCheck { def is = s2"""
  Take the count of an arbitrary values                       $countBy
"""

  def countBy = prop((l: List[(String, Byte)]) => {
    val xs = l.toMap
    val xss = xs.toList.flatMap(x => (0 until Math.abs(x._2)).map(_ => x._1))
    ReducerUtil.run(new CountByReducer[String], xss).map.asScala.toMap ==== xs.mapValues(Math.abs(_).toLong).filter(_._2 > 0)
  })
}
