package com.ambiata.ivory.storage.control

import com.ambiata.ivory.storage.repository.Repository
import com.ambiata.mundane.control._
import scalaz._, Scalaz._

/* Specialised Kleisli for doing common Ivory operations */
case class IvoryT[F[+_], +A](run: Kleisli[F, IvoryRead, A]) {

  def map[B](f: A => B)(implicit F: Functor[F]): IvoryT[F, B] =
    IvoryT(run.map(f))

  def flatMap[B](f: A => IvoryT[F, B])(implicit F: Monad[F]): IvoryT[F, B] =
    IvoryT(run.flatMap(f(_).run))
}

object IvoryT {

  def repository[F[+_]: Monad]: IvoryT[F, Repository] =
     IvoryT(Kleisli.ask[F, IvoryRead].map(_.repository))

  def fromResultT[F[+_], A](r: ResultT[F, A]): IvoryT[({ type l[+a] = ResultT[F, a] })#l, A] = {
    type X[+B] = ResultT[F, B]
    IvoryT[X, A](Kleisli[X, IvoryRead, A](_ => r))
  }

  def fromResultTIO[A](f: Repository => ResultTIO[A]): IvoryTIO[A] =
    IvoryT(Kleisli[ResultTIO, IvoryRead, A](r => f(r.repository)))

  implicit def IvoryTMonad[F[+_]: Monad]: Monad[({ type l[a] = IvoryT[F, a] })#l] =
    new Monad[({ type l[a] = IvoryT[F, a] })#l] {
      def point[A](v: => A) = IvoryT(Kleisli(_ => v.point[F]))
      def bind[A, B](m: IvoryT[F, A])(f: A => IvoryT[F, B]) = m.flatMap(f)
    }
}

/* Currently only holds a Repository, but will be extended to include trace/profile objects */
case class IvoryRead(repository: Repository)

object IvoryRead {

  // This will eventually have more arguments and we want to know what breaks
  def prod(repository: Repository): IvoryRead =
    IvoryRead(repository)

  /* Do _NOT_ use this for non-test code - as we start adding more parameters we want compiler errors */
  def testing(repository: Repository): IvoryRead =
    IvoryRead(repository)
}
