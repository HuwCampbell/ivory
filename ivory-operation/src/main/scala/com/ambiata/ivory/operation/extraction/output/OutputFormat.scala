package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._

sealed trait OutputFormat {

  def format: OutputFileFormat = this match {
    case DenseFormat(f) => f
    case SparseFormat(f) => f
  }
}
case class DenseFormat(file: OutputFileFormat) extends OutputFormat
case class SparseFormat(file: OutputFileFormat) extends OutputFormat

sealed trait OutputFileFormat
case class DelimitedFile(delim: Char) extends OutputFileFormat
case class EscapedFile(delim: Char) extends OutputFileFormat
case object ThriftFile extends OutputFileFormat

object OutputFormat {

  def fromString(s: String): Option[OutputFormat] = PartialFunction.condOpt(s)({
    case "dense:psv"               => DenseFormat(DelimitedFile('|'))
    case "dense:csv"               => DenseFormat(DelimitedFile(','))
    case "dense:tsv"               => DenseFormat(DelimitedFile('\t'))
    case "dense:thrift"            => DenseFormat(ThriftFile)
    case "sparse:psv"              => SparseFormat(DelimitedFile('|'))
    case "sparse:csv"              => SparseFormat(DelimitedFile(','))
    case "sparse:tsv"              => SparseFormat(DelimitedFile('\t'))
    case "sparse:thrift"           => SparseFormat(ThriftFile)

    case "dense:delimited:psv"     => DenseFormat(DelimitedFile('|'))
    case "dense:delimited:csv"     => DenseFormat(DelimitedFile(','))
    case "dense:delimited:tsv"     => DenseFormat(DelimitedFile('\t'))
    case "dense:delimited:thrift"  => DenseFormat(ThriftFile)
    case "sparse:delimited:psv"    => SparseFormat(DelimitedFile('|'))
    case "sparse:delimited:csv"    => SparseFormat(DelimitedFile(','))
    case "sparse:delimited:tsv"    => SparseFormat(DelimitedFile('\t'))
    case "sparse:delimited:thrift" => SparseFormat(ThriftFile)

    case "dense:escaped:psv"       => DenseFormat(EscapedFile('|'))
    case "dense:escaped:csv"       => DenseFormat(EscapedFile(','))
    case "dense:escaped:tsv"       => DenseFormat(EscapedFile('\t'))
    case "dense:escaped:thrift"    => DenseFormat(ThriftFile)
    case "sparse:escaped:psv"      => SparseFormat(EscapedFile('|'))
    case "sparse:escaped:csv"      => SparseFormat(EscapedFile(','))
    case "sparse:escaped:tsv"      => SparseFormat(EscapedFile('\t'))
    case "sparse:escaped:thrift"   => SparseFormat(ThriftFile)
  })
}

case class OutputFormats(outputs: List[(OutputFormat, IvoryLocation)], missingValue: String)
