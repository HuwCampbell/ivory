package com.ambiata.ivory.storage.control

import com.ambiata.ivory.core.Repository
import com.ambiata.mundane.control._
import scalaz._, Scalaz._, effect.IO

/* Specialised Kleisli similar to IvoryT, enriched with a repository in the reader. */
case class RepositoryT[F[_], A](run: Kleisli[F, RepositoryRead, A]) {
  def toIvoryT(repository: Repository): IvoryT[F, A] =
    IvoryT(Kleisli(i => run(RepositoryRead.fromIvoryRead(repository, i))))

  def map[B](f: A => B)(implicit F: Functor[F]): RepositoryT[F, B] =
    RepositoryT(run.map(f))

  def flatMap[B](f: A => RepositoryT[F, B])(implicit F: Monad[F]): RepositoryT[F, B] =
    RepositoryT(run.flatMap(f(_).run))
}

object RepositoryT {
  def repository[F[_]: Monad]: RepositoryT[F, Repository] =
     RepositoryT(Kleisli.ask[F, RepositoryRead].map(_.repository))

  def fromResultT[F[_], A](f: Repository => ResultT[F, A]): RepositoryT[({ type l[a] = ResultT[F, a] })#l, A] = {
    type X[B] = ResultT[F, B]
    RepositoryT[X, A](Kleisli[X, RepositoryRead, A](r => f(r.repository)))
  }

  def fromRIO[A](f: Repository => RIO[A]): RepositoryT[RIO, A] =
    RepositoryT[RIO, A](Kleisli[RIO, RepositoryRead, A](r => f(r.repository)))

  def fromIvoryT[F[_], A](f: Repository => IvoryT[({ type l[a] = ResultT[F, a] })#l, A]): RepositoryT[({ type l[a] = ResultT[F, a] })#l, A] = {
    type X[B] = ResultT[F, B]
    RepositoryT[X, A](Kleisli[X, RepositoryRead, A](r => f(r.repository).run.run(r.ivory)))
  }

  def fromIvoryTIO[A](f: Repository => IvoryTIO[A]): RepositoryT[RIO, A] =
    RepositoryT[RIO, A](Kleisli[RIO, RepositoryRead, A](r => f(r.repository).run.run(r.ivory)))

  implicit def RepositoryTMonad[F[_]: Monad]: Monad[({ type l[a] = RepositoryT[F, a] })#l] =
    new Monad[({ type l[a] = RepositoryT[F, a] })#l] {
      def point[A](v: => A) = RepositoryT(Kleisli(_ => v.point[F]))
      def bind[A, B](m: RepositoryT[F, A])(f: A => RepositoryT[F, B]) = m.flatMap(f)
    }
}

case class RepositoryRead(repository: Repository, ivory: IvoryRead)

object RepositoryRead {
  def fromRepository(repository: Repository): RIO[RepositoryRead] =
    IvoryRead.createIO.map(RepositoryRead(repository, _))

  def fromIvoryT[F[_]: Monad](repository: Repository): IvoryT[F, RepositoryRead] =
    IvoryT.read[F].map(fromIvoryRead(repository, _))

  def fromIvoryTIO(repository: Repository): IvoryT[RIO, RepositoryRead] =
    fromIvoryT[RIO](repository)

  def fromIvoryRead(repository: Repository, read: IvoryRead): RepositoryRead =
    RepositoryRead(repository, read)
}
