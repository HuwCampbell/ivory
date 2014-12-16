package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.mundane.parse._, ListParser._

import org.joda.time.DateTimeZone

import scalaz.{Name => _, Value => _, _}, Scalaz._

// FIX should be in storage.fact
object EavtParsers {

  def splitLine(delim: Char, line: String): List[String] =
    line.split(delim).toList match {
      case e :: a :: v :: t :: Nil => List(e, a, v, t.trim)
      case other                   => other
    }

  def parser(dictionary: Dictionary, namespace: Name, ivoryTimezone: DateTimeZone, ingestTimezone: DateTimeZone): ListParser[Fact] =
    fact(dictionary, namespace, ivoryTimezone, ingestTimezone)

  def fact(dictionary: Dictionary, namespace: Name, ivoryTimezone: DateTimeZone, ingestTimezone: DateTimeZone): ListParser[Fact] =
    for {
      entity    <- string.nonempty
      name      <- string.nonempty
      rawv      <- string
      v         <- value(validateFeature(dictionary, FeatureId(namespace, name), rawv))
      time      <- Dates.parser(ingestTimezone, ivoryTimezone)
    } yield time match {
      case \/-(dt) =>
        Fact.newFactWithNamespaceName(entity, namespace, name, dt.date, dt.time, v)
      case -\/(d) =>
        Fact.newFactWithNamespaceName(entity, namespace, name, d, Time(0), v)
    }

  def validateFeature(dict: Dictionary, fid: FeatureId, rawv: String): Validation[String, Value] =
    dict.byFeatureId.get(fid).map {
      case Concrete(_, fm) => EavtParsers.valueFromString(fm, rawv)
      case Virtual(_, _)   => s"Cannot import virtual feature $fid".failure
    }.getOrElse(s"Could not find dictionary entry for '${fid}'".failure)

  def valueFromString(meta: ConcreteDefinition, raw: String): Validation[String, Value] = meta.encoding match {
    case _ if meta.tombstoneValue.contains(raw)  => TombstoneValue.success[String]
    case p: PrimitiveEncoding                    => Value.parsePrimitive(p, raw)
    case s: StructEncoding                       => "Struct encoding not supported".failure
    case _: ListEncoding                         => "List encoding not supported".failure
  }
}
