package com.ambiata.ivory.core.gen

import com.ambiata.ivory.core._
import org.scalacheck.Gen

object GenFileFormat {

  def delimiter: Gen[Delimiter] =
    Gen.oneOf(Delimiter.Psv, Delimiter.Tsv, Delimiter.Csv)

  def encoding: Gen[TextEscaping] =
    Gen.oneOf(TextEscaping.Delimited, TextEscaping.Escaped)

  def form: Gen[Form] =
    Gen.oneOf(Form.Dense, Form.Sparse)

  def format: Gen[FileFormat] = for {
    d <- delimiter
    e <- encoding
    o <- Gen.oneOf(FileFormat.Text(d, e), FileFormat.Thrift)
  } yield o

  def output: Gen[OutputFormat] = for {
    o <- form
    f <- format
  } yield OutputFormat(o, f)
}
