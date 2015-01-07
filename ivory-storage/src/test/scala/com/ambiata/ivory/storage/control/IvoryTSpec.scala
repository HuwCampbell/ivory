package com.ambiata.ivory.storage.control

import com.ambiata.mundane.testing.Laws._
import org.scalacheck._, Arbitrary._
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.NoCanBeEqual
import scalaz._, Scalaz._

class IvoryTSpec extends Specification with ScalaCheck with NoCanBeEqual { def is = s2"""

IvoryT Laws
===========

  monad laws                     ${monad.laws[IvoryOption]}
  on works                       ${on}
"""

  type IvoryOption[A] = IvoryT[Option, A]

  def on = prop((ivoryT: IvoryOption[Int], i: Option[Int]) =>
    ivoryT.on(_.flatMap(_ => i)) === IvoryT(Kleisli(_ => ivoryT.run.run(IvoryRead.create).flatMap(_ => i))))

  implicit def IvoryTEqual[M[_]](implicit M: Equal[M[Int]]): Equal[IvoryT[M, Int]] = new Equal[IvoryT[M, Int]] {
    def equal(a1: IvoryT[M, Int], a2: IvoryT[M, Int]): Boolean = {
      val read = IvoryRead.create
      val mb1: M[Int] = a1.run(read)
      val mb2: M[Int] = a2.run(read)
      M.equal(mb1, mb2)
    }
  }

  implicit def IvoryTArbitrary[F[ _], A](implicit F: Functor[F], A: Arbitrary[F[A]]): Arbitrary[IvoryT[F, A]] = {
    Arbitrary(A.arbitrary.map(f => IvoryT(Kleisli(_ => f))))
  }
}
