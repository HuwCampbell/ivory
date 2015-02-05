package com.ambiata.ivory.storage.parse

import com.ambiata.ivory.core._
import com.ambiata.mundane.parse._, ListParser._

import org.joda.time.DateTimeZone

import scalaz.{Value => _, _}, Scalaz._

// FIX should be in storage.fact
object EavtParsers {

  def splitLine(delim: Char, line: String): List[String] =
    line.split(delim).toList match {
      case e :: a :: v :: t :: Nil => List(e, a, v, t.trim)
      case other                   => other
    }

  /** WARNING: This isn't symmetrical yet - we don't handle lists/structs on ingest (yet) */
  def toEavt(fact: Fact, tombstone: String): List[String] =
    List(fact.entity, fact.feature, Value.toStringWithStruct(fact.value, tombstone), fact.datetime.localIso8601)

  def toEavtDelimited(fact: Fact, tombstone: String, delim: Char): String =
    toEavt(fact, tombstone).mkString(delim.toString)

  def parser(dictionary: Dictionary, namespace: Namespace, ivoryTimezone: DateTimeZone, ingestTimezone: DateTimeZone): ListParser[Fact] =
    fact(dictionary, namespace, ivoryTimezone, ingestTimezone)

  def fact(dictionary: Dictionary, namespace: Namespace, ivoryTimezone: DateTimeZone, ingestTimezone: DateTimeZone): ListParser[Fact] = for {
    entity <- string.nonempty
    name   <- string.nonempty
    rawv   <- string
    v      <- value(validateFeature(dictionary, FeatureId(namespace, name), rawv))
    time   <- Dates.parser(ingestTimezone, ivoryTimezone)
    f       = time match {
                case \/-(dt) => Fact.newFactWithNamespace(entity, namespace, name, dt.date, dt.time, v)
                case -\/(d)  => Fact.newFactWithNamespace(entity, namespace, name, d, Time(0), v)
              }
    fact   <- value(Value.validateFact(f, dictionary))
  } yield fact

  def validateFeature(dict: Dictionary, fid: FeatureId, rawv: String): Validation[String, Value] =
    dict.byFeatureId.get(fid).map {
      case Concrete(_, fm) => EavtParsers.valueFromString(fm, rawv)
      case Virtual(_, _)   => s"Cannot import virtual feature $fid".failure
    }.getOrElse(s"Could not find dictionary entry for '${fid}'".failure)

  def valueFromString(meta: ConcreteDefinition, raw: String): Validation[String, Value] =
    if (meta.tombstoneValue.contains(raw)) TombstoneValue.success[String]
    else  meta.encoding.fold(
      p => Value.parsePrimitive(p, raw),
      _ => "Struct encoding not supported".failure,
      _ => "List encoding not supported".failure
  )
}
