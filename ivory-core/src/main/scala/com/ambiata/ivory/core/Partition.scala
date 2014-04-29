package com.ambiata.ivory.core

import scalaz._, Scalaz._
import java.io.File
import com.ambiata.mundane.parse.ListParser

object Partition {

  type FactSetName = String
  type Namespace = String

  def parseFilename(file: File): Validation[String, (FactSetName, Namespace, Date)] =
    parseWith(file.toURI.getPath)

  def parseWith(f: => String): Validation[String, (FactSetName, Namespace, Date)] =
    pathParser.run(f.split("/").toList.reverse)

  def pathParser: ListParser[(FactSetName, Namespace, Date)] = {
    import ListParser._
    for {
      _       <- consume(1)
      date    <- Date.listParser
      ns      <- string
      factset <- string
      _       <- consumeRest
    } yield (factset, ns, date)
  }

  def path(ns: Namespace, date: Date): String = {
    ns + "/" + "%4d/%02d/%02d".format(date.year, date.month, date.day)
  }
}
