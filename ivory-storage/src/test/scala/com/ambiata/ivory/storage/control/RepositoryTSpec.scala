package com.ambiata.ivory.storage.control

import com.ambiata.mundane.testing.Laws._
import com.ambiata.ivory.core._
import com.ambiata.notion.core._
import org.scalacheck._, Arbitrary._
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._

class RepositoryTSpec extends Specification with ScalaCheck { def is = s2"""

RepositoryT Laws
===========

  monad laws                     ${monad.laws[RepositoryOption]}

"""
  type RepositoryOption[A] = RepositoryT[Option, A]

  implicit def RepositoryTEqual[M[_]](implicit M: Equal[M[Int]]): Equal[RepositoryT[M, Int]] = new Equal[RepositoryT[M, Int]] {
    def equal(a1: RepositoryT[M, Int], a2: RepositoryT[M, Int]): Boolean = {
      val read = RepositoryRead.fromIvoryRead(LocalRepository(LocalLocation("/"), IvoryFlags.default), IvoryRead.create)
      val mb1: M[Int] = a1.run(read)
      val mb2: M[Int] = a2.run(read)
      M.equal(mb1, mb2)
    }
  }

  implicit def RepositoryTArbitrary[F[ _], A](implicit F: Functor[F], A: Arbitrary[F[A]]): Arbitrary[RepositoryT[F, A]] = {
    Arbitrary(A.arbitrary.map(f => RepositoryT(Kleisli(_ => f))))
  }
}
