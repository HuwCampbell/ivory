package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core.{ParseError => _, _}
import com.ambiata.mundane.control._
import com.ambiata.mundane.parse._
import com.ambiata.notion.core.Location
import org.parboiled2._, Parser.DeliveryScheme.Either
import scalaz.effect.IO
import scalaz.{Name =>_,_}, Scalaz._, Validation._
import shapeless._

object DictionaryTextStorageV2 extends TextStorage[(FeatureId, Definition), Dictionary] {

  val name = "dictionary"
  val DELIM = "|"

  def fromList(entries: List[(FeatureId, Definition)]): ValidationNel[String, Dictionary] =
    Validation.success(Dictionary(entries.map(_._2)))

  def dictionaryFromIvoryLocation(location: IvoryLocation): ResultTIO[Dictionary] =
    fromIvoryLocation(location).map(Dictionary.reduce)

  def toList(dict: Dictionary): List[(FeatureId, Definition)] =
    dict.definitions.map(d => d.featureId -> d).toList

  def parseLine(i: Int, l: String): ValidationNel[String, (FeatureId, Definition)] =
    DictionaryTextStorageV2(l, DELIM).parse

  def toLine(f: (FeatureId, Definition)) = f._1.toString(":") + DELIM + (f._2 match {
    case Concrete(_, ConcreteDefinition(encoding, ty, desc, tombstone)) => List(
      Some("encoding" -> Encoding.render(encoding)),
      ty.flatMap(t => Some("type" -> Type.render(t))),
      if (desc.isEmpty) None else Some("description" -> desc),
      if (tombstone.isEmpty) None else Some("tombstone" -> tombstone.mkString(","))
    )
    case Virtual(_, d) => List(
      Some("source" -> d.source.toString(":")),
      Some("expression" -> Expression.asString(d.query.expression)),
      d.query.filter.map(_.render).map("filter" ->),
      d.window.map(Window.asString).map("window" ->)
    )
  }).flatten.map { case (k, v) => k + "=" + v}.mkString(DELIM)

  def parseEncoding(encv: String): ValidationNel[String, Encoding] =
    DictionaryTextStorage.parseEncoding(encv).toValidationNel |||
      DictionaryTextStorageV2(encv, DELIM).parseList |||
      DictionaryTextStorageV2(encv, DELIM).parseStruct
}

case class DictionaryTextStorageV2(input: ParserInput, DELIMITER: String) extends Parser {

  private def alpha            = rule(anyOf(('a' to 'z').mkString+('A' to 'Z').mkString))
  private def num              = rule(anyOf("0123456789"))
  private def separator        = rule(anyOf("-_"))
  private def alphaNum         = rule(alpha | num)
  private def alphaNumSep      = rule(capture(oneOrMore(alphaNum | '-') ~ zeroOrMore(alpha | num | separator)))
  private def txt(d: String)   = rule(capture(!anyOf(d) ~ ANY))
  private def entry(d: String) = rule(zeroOrMore(txt(d)) ~> (_.mkString("")))
  private def nameEntry        = rule(alphaNumSep ~> (values => Name.nameFromStringDisjunction(values.mkString(""))))
  private def mapEnty          = rule(zeroOrMore((entry("=") ~ "=" ~ entry(DELIMITER)) ~> ((k, v) => (k.trim, v.trim))).separatedBy(DELIMITER))
  private def map              = rule(mapEnty ~> (_.toMap))
  private def featureId        = rule(nameEntry ~ ":" ~ entry(DELIMITER) ~> ((ns, n) => ns.map(FeatureId(_, n))))
  private def row              = rule(featureId ~ optional(DELIMITER ~ map))

  private def structEntry      = rule(zeroOrMore((entry(":") ~ ":" ~ entry(",*)") ~ capture(optional("*"))) ~> ((k, v, o) => (k.trim, v.trim) -> (o == "*"))).separatedBy(","))
  private def struct           = rule("(" ~ structEntry ~ ")")
  private def list             = rule("[" ~ entry("]") ~ "]")

  private def parseStruct: ValidationNel[String, StructEncoding] = struct.run().fold(
    formatError(_).failureNel, _.toList.traverseU {
      case ((k, v), o) => DictionaryTextStorage.parseEncoding(v).map(enc => k -> StructEncodedValue(enc, optional = o)).toValidationNel
    }.map(s => StructEncoding(s.toMap))
  )

  private def parseList: ValidationNel[String, Encoding] = list.run().fold(formatError(_).failureNel, s =>
    (DictionaryTextStorage.parseEncoding(s).toValidationNel ||| DictionaryTextStorageV2(s, DELIMITER).parseStruct).map(ListEncoding)
  )

  private def metaFromMap(featureId: FeatureId, m: Map[String, String]): ValidationNel[String, Definition] = {
    (m.get("encoding"), m.get("source")) match {
      case (Some(encv), None) =>
        val enc = DictionaryTextStorageV2.parseEncoding(encv)
        val ty = m.get("type").cata(DictionaryTextStorage.parseType(_).map(some), None.success).toValidationNel
        val desc = m.getOrElse("description", "")
        val tomb = m.get("tombstone").cata(Delimited.parseCsv, Nil)
        (enc |@| ty)(Definition.concrete(featureId, _, _, desc, tomb))
      case (None, Some(source)) =>
        val window = m.get("window").traverseU { s =>
           s.split(" ", 2) match {
             case Array(len, unitStr) =>
               val length = len.parseInt.disjunction.leftMap(_ => s"Invalid duration number format $len")
               val unit = Window.unitFromString(unitStr).toRightDisjunction(s"Invalid unit type $unitStr")
               (length |@| unit)(Window.apply)
             case _  =>
               s"Invalid duration format $s".left
           }
        }
        val expression = m.get("expression").toRightDisjunction("Missing expression").flatMap(Expression.parse)
        val filter = m.get("filter").map(Filter.apply).right
        DictionaryTextStorageV2(source, DELIMITER).featureId.run().fold(formatError(_).failureNel,
          fid => (fid |@| (expression |@| filter)(Query.apply) |@| window)(Definition.virtual(featureId, _, _, _)).validation.toValidationNel)
      case (Some(_), Some(_))  =>
        "Must specify either 'encoding' or 'source' but not both".failureNel
      case (None, None)        =>
        "Must either specify at least 'encoding' or 'source'".failureNel
    }
  }

  def parse: ValidationNel[String, (FeatureId, Definition)] =
    row.run().fold(formatError(_).failureNel, {
      case \/-(featureId) :: m :: HNil => metaFromMap(featureId, m.getOrElse(Map())).map(featureId ->)
      case -\/(m) :: _                 => m.failureNel
    })
}
