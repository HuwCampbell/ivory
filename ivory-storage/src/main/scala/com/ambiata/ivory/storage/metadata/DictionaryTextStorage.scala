package com.ambiata.ivory.storage.metadata

import scalaz.{Value => _, _}, Scalaz._, \&/._, effect.IO
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.ivory.storage.store._

object DictionaryTextStorage {

  def dictionaryFromHdfs[F[+_] : Monad](path: StorePathResultT[F]): ResultT[F, Dictionary] = for {
    exists <- path.run(_.exists)
    _      <- if (!exists) ResultT.fail[F, Unit](s"Path ${path.path} does not exist in ${path.store}!") else ResultT.ok[F, Unit](())
    lines  <- path.run(_.linesUtf8.read)
    dict   <- ResultT.fromDisjunction[F, Dictionary](fromLines(lines).leftMap(\&/.This(_)))
  } yield dict

  def dictionaryToHdfs(path: Path, dict: Dictionary, delim: Char ='|'): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, delimitedDictionaryString(dict, delim)))

  def fromInputStream(is: java.io.InputStream): ResultTIO[Dictionary] = for {
    content <- Streams.read(is)
    r <- ResultT.fromDisjunction[IO, Dictionary](fromLines(content.lines.toList).leftMap(This(_)))
  } yield r

  def fromString(s: String): String \/ Dictionary =
    fromLines(s.lines.toList)

  def fromLines(lines: List[String]): String \/ Dictionary = {
    val numbered = lines.zipWithIndex.map({ case (l, n) => (l, n + 1) })
    numbered.map({ case (l, n) => parseDictionaryEntry(l).leftMap(e => s"Line $n: $e")}).sequenceU.map(entries => Dictionary(entries.toMap))
  }

  def fromFile(path: String): ResultTIO[Dictionary] = {
    val file = new java.io.File(path)
    for {
      raw <- Files.read(file.getAbsolutePath.toFilePath)
      fs  <- ResultT.fromDisjunction[IO, Dictionary](fromLines(raw.lines.toList).leftMap(err => This(s"Error reading dictionary from file '$path': $err")))
    } yield fs
  }

  def delimitedDictionaryString(dict: Dictionary, delim: Char): String = {
    val strDelim = delim.toString
    dict.meta.map({ case (featureId, featureMeta) =>
      featureId.toString(strDelim) + delim + featureMeta.toString(strDelim)
    }).mkString("\n") + "\n"
  }

  def parseDictionaryEntry(entry: String): String \/ (FeatureId, FeatureMeta) = {
    import ListParser._
    val parser: ListParser[(FeatureId, FeatureMeta)] = for {
      namespace <- string
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
    } yield (FeatureId(namespace, name), FeatureMeta(encoding, ty, desc, Delimited.parseCsv(tombstone)))
    parser.run(Delimited.parsePsv(entry)).disjunction
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
