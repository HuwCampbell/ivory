package com.ambiata.ivory.core

import scalaz._, Scalaz._

object OptionTPlus {

  def fromOption[M[_]: Applicative, A](a: Option[A]): OptionT[M, A] =
    OptionT(a.pure[M])

  def when[M[_]: Applicative, A](b: Boolean, a: => A): OptionT[M, A] =
    if (b) OptionT.some(a) else OptionT.none
}
