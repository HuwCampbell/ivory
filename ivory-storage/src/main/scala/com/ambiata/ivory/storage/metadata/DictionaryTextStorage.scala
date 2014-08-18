package com.ambiata.ivory.storage.metadata

import scalaz.{Name => _, Value => _, _}, Scalaz._
import com.ambiata.mundane.parse._
import com.ambiata.ivory.core._

trait DictionaryTextStorageCommon extends TextStorage[(FeatureId, FeatureMeta), Dictionary] {

  val name = "dictionary"

  def fromList(entries: List[(FeatureId, FeatureMeta)]): Dictionary =
    Dictionary(entries.toMap)

  def toList(d: Dictionary): List[(FeatureId, FeatureMeta)] =
    d.meta.toList
}

object DictionaryTextStorage extends DictionaryTextStorageCommon {

  val DELIM = "|"

  def parseLine(i: Int, e: String): ValidationNel[String, (FeatureId, FeatureMeta)] =
    parseDictionaryEntry(e).toValidationNel

  def toLine(f: (FeatureId, FeatureMeta)): String =
    delimitedLineWithDelim(f, DELIM)

  def delimitedLineWithDelim(f: (FeatureId, FeatureMeta), delim: String): String =
    f._1.toString(DELIM) + DELIM + metaToString(f._2, DELIM)

  private def metaToString(meta: FeatureMeta, delim: String): String = {
    import meta._
    s"${Encoding.render(encoding)}${delim}${ty.map(Type.render).getOrElse("")}${delim}${desc}${delim}${tombstoneValue.mkString(",")}"
  }

  def parseDictionaryEntry(entry: String): Validation[String, (FeatureId, FeatureMeta)] = {
    import ListParser._
    val parser: ListParser[(FeatureId, FeatureMeta)] = for {
      namespace <- Name.listParser
      name      <- string
      encoding  <- for {
        s <- string
        r <- value(parseEncoding(s))
      } yield r
      ty        <- for {
        s <- string
        r <- value(parseType(s))
      } yield r
      desc      <- string
      tombstone <- string
    } yield (FeatureId(namespace, name), FeatureMeta(encoding, Some(ty), desc, Delimited.parseCsv(tombstone)))
    parser.run(Delimited.parsePsv(entry))
  }

  def parseEncoding(s: String): Validation[String, PrimitiveEncoding] =
    s match {
      case "boolean"    => BooleanEncoding.success
      case "int"        => IntEncoding.success
      case "long"       => LongEncoding.success
      case "double"     => DoubleEncoding.success
      case "string"     => StringEncoding.success
      case otherwise    => s"""not a valid encoding: '$s'""".failure
    }

  def parseType(s: String): Validation[String, Type] =
    s match {
      case "numerical"    => NumericalType.success
      case "continuous"   => ContinuousType.success
      case "categorical"  => CategoricalType.success
      case "binary"       => BinaryType.success
      case otherwise      => s"not a valid feature type: '$s'".failure
    }

}
