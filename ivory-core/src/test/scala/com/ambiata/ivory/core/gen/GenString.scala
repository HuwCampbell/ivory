package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._
import org.scalacheck._

object GenString {
  def name: Gen[Name] =
    Gen.frequency(
      9 -> sensible.map(_.take(20)),
      // Ideally this is 255 but we often append strings to names to guarantee uniqueness
      1 -> sensible.map(_.take(200))
    ).map(Name.reviewed)

  def word: Gen[String] =
    Gen.identifier

  def words: Gen[List[String]] =
    Gen.listOf(word)

  def sentence: Gen[String] =
    words.map(_.mkString(" "))

  def sensible: Gen[String] = for {
    h <- first
    t <- rest
  } yield (h :: t).mkString

  def first: Gen[Char] =
    Gen.alphaNumChar

  def rest: Gen[List[Char]] = Gen.listOf(Gen.frequency(
    2 -> Gen.const('_')
  , 2 -> Gen.const('-')
  , 96 -> Gen.alphaNumChar
  ))

  // FIX ARB this isn't good enough. Investigate context and improve.
  def badName: Gen[String] =
    Gen.oneOf("", "_name", "name1/name2", "name㭊")
}
