package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.store._
import ResultT._
import com.ambiata.mundane.data.Lists

import scalaz.{Value => _, _}, Scalaz._, effect.IO

trait TextStorage[L, T] {

  def name: String
  def parseLine(i: Int, l: String): ValidationNel[String, L]
  def fromList(s: List[L]): ValidationNel[String, T]
  def toList(t: T): List[L]
  def toLine(l: L): String

  def fromSingleFile(location: IvoryLocation): ResultTIO[T] = for {
    lines  <- IvoryLocation.readLines(location)
    result <- ResultT.fromDisjunctionString[IO, T](fromLines(lines))
  } yield result

  /**
   * Read text data from a Location that is either a directory containing files
   * or just a single file
   */
  def fromIvoryLocation(location: IvoryLocation): ResultTIO[List[T]] =
    IvoryLocation.isDirectory(location).flatMap { isDirectory =>
      if (isDirectory) IvoryLocation.list(location).flatMap(_.traverseU(fromSingleFile))
      else             fromSingleFile(location).map(t => List(t))
    }

  def toKeyStore(repository: Repository, key: Key, t: T): ResultTIO[Unit] =
    repository.store.utf8.write(key, delimitedString(t))

  def fromKeyStore(repository: Repository, key: Key): ResultTIO[T] =
    repository.store.linesUtf8.read(key).flatMap(lines => ResultT.fromDisjunctionString[IO, T](fromLines(lines)))

  def fromKeysStore(repository: Repository, key: Key): ResultTIO[List[T]] = for {
    keys <- repository.store.list(key)
    ts   <- keys.traverseU(k => fromKeyStore(repository, key / k))
  } yield ts

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
