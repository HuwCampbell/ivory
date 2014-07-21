package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core.{ParseError => _, _}
import com.ambiata.mundane.parse._
import org.parboiled2._, Parser.DeliveryScheme.Either
import scalaz._, Scalaz._, Validation._
import shapeless._

object DictionaryTextStorageV2 extends DictionaryTextStorageCommon {

  val DELIM = "|"

  def parseLine(i: Int, l: String): ValidationNel[String, (FeatureId, FeatureMeta)] =
    DictionaryTextStorageV2(l, DELIM).parse

  def toLine(f: (FeatureId, FeatureMeta)) =
    f._1.toString(":") + DELIM + metaToString(f._2)

  private def metaToString(meta: FeatureMeta): String = {
    import meta._
    List(
      Some("encoding" -> Encoding.render(encoding)),
      ty.flatMap(t => Some("type" -> Type.render(t))),
      if (desc.isEmpty) None else Some("description" -> desc),
      if (tombstoneValue.isEmpty) None else Some("tombstone" -> tombstoneValue.mkString(","))
    ).flatten.map { case (k, v) => k + "=" + v}.mkString(DELIM)
  }
}

case class DictionaryTextStorageV2(input: ParserInput, DELIMITER: String) extends Parser {

  private def txt(d: String)   = rule(capture(!anyOf(d) ~ ANY))
  private def entry(d: String) = rule(zeroOrMore(txt(d)) ~> (_.mkString("")))
  private def mapEnty          = rule(zeroOrMore((entry("=") ~ "=" ~ entry(DELIMITER)) ~> ((k, v) => (k.trim, v.trim))).separatedBy(DELIMITER))
  private def map              = rule(mapEnty ~> (_.toMap))
  private def featureId        = rule(entry(":") ~ ":" ~ entry(DELIMITER) ~> ((ns, n) => FeatureId(ns, n)))
  private def row              = rule(featureId ~ optional(DELIMITER ~ map))

  private def structEntry      = rule(zeroOrMore((entry(":") ~ ":" ~ entry(",*)") ~ capture(optional("*"))) ~> ((k, v, o) => (k.trim, v.trim) -> (o == "*"))).separatedBy(","))
  private def struct           = rule("(" ~ structEntry ~ ")")

  private def parseStruct: ValidationNel[String, StructEncoding] = struct.run().fold(
    formatError(_).failureNel, _.toList.traverseU {
      case ((k, v), o) => DictionaryTextStorage.parseEncoding(v).map(enc => k -> StructValue(enc, optional = o)).toValidationNel
    }.map(s => StructEncoding(s.toMap))
  )

  private def metaFromMap(m: Map[String, String]): ValidationNel[String, FeatureMeta] = {
    val enc = m.get("encoding").map { s =>
      DictionaryTextStorage.parseEncoding(s).toValidationNel ||| DictionaryTextStorageV2(s, DELIMITER).parseStruct
    }.getOrElse("Encoding not specified".failureNel)
    val ty = m.get("type").cata(DictionaryTextStorage.parseType(_).map(some), None.success).toValidationNel
    val desc = m.getOrElse("description", "")
    val tomb = m.get("tombstone").cata(Delimited.parseCsv, Nil)
    (enc |@| ty)(new FeatureMeta(_, _, desc, tomb))
  }

  def parse: ValidationNel[String, (FeatureId, FeatureMeta)] =
    row.run().fold(formatError(_).failureNel, {
      case featureId :: m :: HNil => metaFromMap(m.getOrElse(Map())).map(featureId ->)
    })
}
