package com.ambiata.ivory.core.gen

import org.scalacheck._


object GenEntity {
  def entity: Gen[String] =
    Gen.choose(0, 10).map(format)

  def entities: Gen[List[String]] =
    Gen.choose(1, 1000).map(listOf)

  def listOf(n: Int): List[String] =
    (1 to n).toList.map(format)

  def format(i: Int): String =
    "T+%05d".format(i)
}
