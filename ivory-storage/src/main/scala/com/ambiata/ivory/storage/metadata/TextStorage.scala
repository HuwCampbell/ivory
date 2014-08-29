package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.data.Lists

import scalaz.{Value => _, _}, Scalaz._, effect.IO

trait TextStorage[L, T] {

  def name: String
  def parseLine(i: Int, l: String): ValidationNel[String, L]
  def fromList(s: List[L]): ValidationNel[String, T]
  def toList(t: T): List[L]
  def toLine(l: L): String

  def fromFileStore[F[+_] : Monad](ref: Reference[F]): F[Result[T]] = for {
    lines  <- ReferenceStore.readLines[F](ref)
  } yield Result.fromDisjunctionString(fromLines(lines))

  def fromFileStoreIO(ref: ReferenceIO): ResultTIO[T] =
    ReferenceStore.readLines(ref).flatMap(lines => ResultT.fromDisjunctionString[IO, T](fromLines(lines)))

  def fromDirStore[F[+_] : Monad](ref: Reference[F]): F[Result[List[T]]] = for {
    files <- ReferenceStore.list[F](ref)
    ts    <- files.traverseU(file => fromFileStore[F](ref </> file))
  } yield ts.sequence

  def toFileStore[F[+_] : Monad](ref: Reference[F], t: T): ResultT[F, Unit] =
    new ResultT[F, Unit](ReferenceStore.writeUtf8[F](ref, delimitedString(t)).as(Result.ok(())))

  def toFileStoreIO(ref: ReferenceIO, t: T): ResultTIO[Unit] =
    ReferenceStore.writeUtf8(ref, delimitedString(t))

  def fromString(s: String): ValidationNel[String, T] =
    fromLinesAll(s.lines.toList)

  def fromLines(lines: List[String]): String \/ T = {
    fromLinesAll(lines).leftMap(_.toList.mkString(",")).disjunction
  }

  def fromLinesAll(lines: List[String]): ValidationNel[String, T] = {
    val numbered = lines.zipWithIndex.map({ case (l, n) => (l, n + 1) })
    numbered.traverseU({ case (l, n) => parseLine(n, l).leftMap(_.map(s"Line $n: " +))}).flatMap(fromList)
  }

  def delimitedString(t: T): String =
    Lists.prepareForFile(toList(t).map(toLine))
}
