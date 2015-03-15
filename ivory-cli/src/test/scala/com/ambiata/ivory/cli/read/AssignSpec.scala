package com.ambiata.ivory.cli.read

import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._

class AssignSpec extends Specification with ScalaCheck { def is = s2"""

  Tuple return the correct values
    $tuple

  Reading an Assign can handle correct strings
    $readPass

  Reading an Assign will fail on incorrect strings
    $readFail

  Reading an Assign will fail on incorrect reads (left)
    $readFailLeft

  Reading an Assign will fail on incorrect reads (right)
    $readFailRight
"""

  def tuple = prop((a: String, b: Int) =>
    Assign(a, b).tuple ==== (a -> b)
  )

  def readPass = prop((a: Int, b: Boolean, l: List[String]) =>
    Assign.AssignRead[Int, Boolean].read(s"$a=$b" :: l) ==== (l, Assign(a, b)).right
  )

  def readFail = prop((a: String, l: List[String]) => !a.contains("=") ==> {
    Assign.AssignRead[Int, Boolean].read(a :: l).toEither must beLeft
  })

  def readFailLeft = prop((a: Int, l: List[String]) =>
    Assign.AssignRead[Int, Boolean].read(s"$a=$a" :: l).toEither must beLeft
  )

  def readFailRight = prop((a: Int, l: List[String]) =>
    Assign.AssignRead[Boolean, Int].read(s"$a=$a" :: l).toEither must beLeft
  )
}
