package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._
import org.scalacheck._

object GenString {
  def namespace: Gen[Namespace] = for {
    colour <- Gen.oneOf(colours)
    number <- Gen.choose(0, 9)
  } yield Namespace.reviewed(s"${colour}${number}")

  def word: Gen[String] =
    Gen.identifier

  def words: Gen[List[String]] =
    // FIX: This isn't really ideal, but be _very_ careful when changing the max size as this can result in
    // _significantly_ slower property generation (see DatasetsSpec)
    GenPlus.listOfSized(0, 5, word)

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

  def colours: List[String] = List(
    "red"
  , "green"
  , "blue"
  , "yellow"
  , "pink"
  , "purple"
  , "orange"
  , "black"
  , "white"
  , "gray"
  )
}
