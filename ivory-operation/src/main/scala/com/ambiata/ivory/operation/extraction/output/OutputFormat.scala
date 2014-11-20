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
case object ThriftFile extends OutputFileFormat

object OutputFormat {

  def fromString(s: String): Option[OutputFormat] = PartialFunction.condOpt(s)({
    case "dense:psv"      => DenseFormat(DelimitedFile('|'))
    case "dense:csv"      => DenseFormat(DelimitedFile(','))
    case "dense:tsv"      => DenseFormat(DelimitedFile('\t'))
    case "dense:thrift"   => DenseFormat(ThriftFile)
    case "sparse:psv"     => SparseFormat(DelimitedFile('|'))
    case "sparse:csv"     => SparseFormat(DelimitedFile(','))
    case "sparse:tsv"     => SparseFormat(DelimitedFile('\t'))
    case "sparse:thrift"  => SparseFormat(ThriftFile)
  })
}

case class OutputFormats(outputs: List[(OutputFormat, OutputDataset)], missingValue: String)
