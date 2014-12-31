package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import RIO._
import com.ambiata.mundane.data.Lists

import scalaz.{Value => _, _}, Scalaz._, effect.IO

trait TextStorage[L, T] {

  def parseLine(i: Int, l: String): ValidationNel[String, L]
  def fromList(s: List[L]): ValidationNel[String, T]
  def toList(t: T): List[L]
  def toLine(l: L): String

  def fromSingleFile(location: IvoryLocation): RIO[T] = for {
    lines  <- IvoryLocation.readLines(location)
    result <- RIO.fromDisjunctionString[T](fromLines(lines))
  } yield result

  /**
   * Read text data from a Location that is either a directory containing files
   * or just a single file
   */
  def fromIvoryLocation(location: IvoryLocation): RIO[List[T]] =
    IvoryLocation.isDirectory(location).flatMap { isDirectory =>
      if (isDirectory) IvoryLocation.list(location).flatMap(_.traverseU(fromSingleFile))
      else             fromSingleFile(location).map(t => List(t))
    }

  def toKeyStore(repository: Repository, key: Key, t: T): RIO[Unit] =
    repository.store.utf8.write(key, delimitedString(t))

  def fromKeyStore(repository: Repository, key: Key): RIO[T] =
    repository.store.linesUtf8.read(key).flatMap(lines => RIO.fromDisjunctionString[T](fromLines(lines)))

  def fromString(s: String): ValidationNel[String, T] =
    fromLinesAll(s.lines.toList)

  def fromLines(lines: List[String]): String \/ T = {
    fromLinesAll(lines).leftMap(_.toList.mkString(",")).disjunction
  }

  def fromLinesAll(lines: List[String]): ValidationNel[String, T] = {
    val numbered = lines.zipWithIndex.map({ case (l, n) => (l, n + 1) })
    import scalaz.Validation.FlatMap._
    numbered.traverseU({ case (l, n) => parseLine(n, l).leftMap(_.map(s"Line $n: " +))}).flatMap(fromList)
  }

  def delimitedString(t: T): String =
    Lists.prepareForFile(toList(t).map(toLine))
}
