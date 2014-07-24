package com.ambiata.ivory.core

import org.specs2._
import scalaz._

class MapsSpec extends Specification with ScalaCheck { def is = s2"""

Maps outerJoin
--------------

  All keys in input appear in output              $keysInput
  All values from left map are in output          $leftMapValues
  All values from right map are in output         $rightMapValues

"""

  import Maps._

  def keysInput = prop((m1: Map[String, String], m2: Map[String, Int]) =>
    outerJoin(m1, m2).keySet ==== (m1.keySet ++ m2.keySet)
  )

  def leftMapValues = prop((m1: Map[String, String], m2: Map[String, Int]) =>
    outerJoin(m1, m2).flatMap(_._2.a).toSet ==== m1.values.toSet
  )

  def rightMapValues = prop((m1: Map[String, String], m2: Map[String, Int]) =>
    outerJoin(m1, m2).flatMap(_._2.b).toSet ==== m2.values.toSet
  )
}
