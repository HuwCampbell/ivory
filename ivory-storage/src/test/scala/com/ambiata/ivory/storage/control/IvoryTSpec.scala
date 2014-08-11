package com.ambiata.ivory.storage.control

import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.Laws._
import org.scalacheck._, Arbitrary._
import org.specs2.{ScalaCheck, Specification}
import scalaz._, Scalaz._

class IvoryTSpec extends Specification with ScalaCheck { def is = s2"""

IvoryT Laws
===========

  monad laws                     ${monad.laws[({type l[a] = IvoryT[Option, a]})#l]}
"""

  implicit def IvoryTEqual[M[+_]](implicit M: Equal[M[Int]]): Equal[IvoryT[M, Int]] = new Equal[IvoryT[M, Int]] {
    def equal(a1: IvoryT[M, Int], a2: IvoryT[M, Int]): Boolean = {
      val read = IvoryRead.testing(Repository.fromLocalPath(FilePath("/")))
      val mb1: M[Int] = a1.run(read)
      val mb2: M[Int] = a2.run(read)
      M.equal(mb1, mb2)
    }
  }

  implicit def IvoryTArbitrary[F[+ _], A](implicit F: Functor[F], A: Arbitrary[F[A]]): Arbitrary[IvoryT[F, A]] = {
    Arbitrary(A.arbitrary.map(f => IvoryT(Kleisli(_ => f))))
  }
}
