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

  def fromStore[F[+_] : Monad](path: ReferenceResultT[F]): ResultT[F, T] = for {
    exists <- path.run(_.exists)
    _      <- if (!exists) ResultT.fail[F, Unit](s"Path ${path.path} does not exist in ${path.store}!") else ResultT.ok[F, Unit](())
    lines  <- path.run(_.linesUtf8.read)
    t      <- ResultT.fromDisjunction[F, T](fromLines(lines).leftMap(\&/.This(_)))
  } yield t

  def toStore[F[+_] : Monad](path: ReferenceResultT[F], t: T): ResultT[F, Unit] =
    path.run(store => path => store.utf8.write(path, delimitedString(t)))

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
