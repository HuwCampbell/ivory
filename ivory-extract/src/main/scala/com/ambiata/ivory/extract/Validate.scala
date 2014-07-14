package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._
import com.ambiata.ivory.core._

object Validate {
  def validateFact(fact: Fact, dict: Dictionary): Validation[String, Fact] =
    dict.meta.get(fact.featureId).map(fm => validateEncoding(fact, fm.encoding)).getOrElse(s"Dictionary entry '${fact.featureId}' doesn't exist!".failure)

  def validateEncoding(fact: Fact, encoding: Encoding): Validation[String, Fact] = {
    val v = fact.value
    (encoding match {
      case BooleanEncoding => if(v.encoding.exists(_ != BooleanEncoding)) s"Not a valid boolean! '${fact.toString}'".failure else Success(())
      case IntEncoding     => if(v.encoding.exists(_ != IntEncoding)) s"Not a valid int! '${fact.toString}'".failure else Success(())
      case LongEncoding    => if(v.encoding.exists(_ != LongEncoding)) s"Not a valid long! '${fact.toString}'".failure else Success(())
      case DoubleEncoding  => if(v.encoding.exists(_ != DoubleEncoding)) s"Not a valid double! '${fact.toString}'".failure else Success(())
      case StringEncoding  => if(v.encoding.exists(_ != StringEncoding)) s"Not a valid string! '${fact.toString}'".failure else Success(())
    }).map(_ => fact)
  }
}
