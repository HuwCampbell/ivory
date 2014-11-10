package com.ambiata.ivory.core

import scalaz._, Scalaz._

object Lists {
  def findMapM[F[_]: Monad, A, B](l: List[A])(f: A => F[Option[B]]): F[Option[B]] = l match {
    case x :: xs =>
      f(x).flatMap({
        case Some(b) =>
          b.some.pure[F]
        case None =>
          findMapM(xs)(f)
      })
    case Nil =>
      none.pure[F]
  }
}
