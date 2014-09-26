package com.ambiata.ivory.operation.extraction.output

import com.ambiata.ivory.core._

sealed trait OutputFormat
case class DenseFormat(delim: Char) extends OutputFormat
case class SparseFormat(delim: Char) extends OutputFormat

object OutputFormat {

  def fromString(s: String): Option[OutputFormat] = PartialFunction.condOpt(s)({
    case "dense:psv"  => DenseFormat('|')
    case "dense:csv"  => DenseFormat(',')
    case "dense:tsv"  => DenseFormat('\t')
    case "sparse:psv" => SparseFormat('|')
    case "sparse:csv" => SparseFormat(',')
    case "sparse:tsv" => SparseFormat('\t')
  })
}

case class OutputFormats(outputs: List[(OutputFormat, IvoryLocation)], missingValue: String)
