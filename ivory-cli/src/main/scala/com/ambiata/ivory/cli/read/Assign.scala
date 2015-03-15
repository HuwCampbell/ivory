package com.ambiata.ivory.cli.read

import pirate._
import scalaz._, Scalaz._

case class Assign[A, B](first: A, second: B) {

  def tuple: (A, B) =
    (first, second)
}

object Assign {

  implicit def AssignRead[A: Read, B: Read]: Read[Assign[A, B]] =
    Read(_ match {
      case Nil =>
        ReadErrorNotEnoughInput.left
      case v :: t =>
        v.split("=", 2).toList match {
          case List(a, b) =>
            (Read.of[A].read(List(a)) |@| Read.of[B].read(List(b)))((x, y) => (x._1 ++ y._1 ++ t) -> Assign(x._2, y._2))
          case l =>
            ReadErrorInvalidType(v, s"Expected A=B but found $l").left
        }
    })
}
