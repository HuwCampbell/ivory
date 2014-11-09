package com.ambiata.ivory.storage.control

import com.ambiata.ivory.core.Repository
import com.ambiata.mundane.control._
import com.ambiata.mundane.trace._
import scalaz._, Scalaz._, effect.IO

/* Specialised Kleisli for doing common Ivory operations */
case class IvoryT[F[_], A](run: Kleisli[F, IvoryRead, A]) {

  def map[B](f: A => B)(implicit F: Functor[F]): IvoryT[F, B] =
    IvoryT(run.map(f))

  def flatMap[B](f: A => IvoryT[F, B])(implicit F: Monad[F]): IvoryT[F, B] =
    IvoryT(run.flatMap(f(_).run))
}

object IvoryT {
  def read[F[_]: Monad]: IvoryT[F, IvoryRead] =
    IvoryT(Kleisli.ask[F, IvoryRead])

  def fromResultT[F[_], A](f: => ResultT[F, A]): IvoryT[({ type l[a] = ResultT[F, a] })#l, A] = {
    type X[B] = ResultT[F, B]
    IvoryT[X, A](Kleisli[X, IvoryRead, A](r => f))
  }

  def fromResultTIO[A](f: => ResultTIO[A]): IvoryT[ResultTIO, A] =
    fromResultT(f)

  implicit def IvoryTMonad[F[_]: Monad]: Monad[({ type l[a] = IvoryT[F, a] })#l] =
    new Monad[({ type l[a] = IvoryT[F, a] })#l] {
      def point[A](v: => A) = IvoryT(Kleisli(_ => v.point[F]))
      def bind[A, B](m: IvoryT[F, A])(f: A => IvoryT[F, B]) = m.flatMap(f)
    }
}

case class IvoryRead(profiler: Profiler[ResultTIO], trace: Trace[ResultTIO])

object IvoryRead {
  // This needs more thought, it would be better to have more knobs around
  // where and how trace gets generated. This will work in the short term
  // though
  def createIO: ResultT[IO, IvoryRead] = for {
    profiler <- Profiler.tree
    trace = Trace.stream(System.out)
  } yield IvoryRead(profiler, trace)

  def create: IvoryRead =
    IvoryRead(Profiler.empty, Trace.stream(System.out))

}
