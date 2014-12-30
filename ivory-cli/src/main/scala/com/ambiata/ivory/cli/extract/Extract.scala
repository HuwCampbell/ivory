package com.ambiata.ivory.cli.extract

import com.ambiata.ivory.api.Ivory.OutputFormats
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import scopt.OptionParser

import scalaz._, Scalaz._

case class ExtractOutput(formats: List[(String, String)] = Nil, missing: String = "NA")

object Extract {

  // This is pretty horrible just for simple re-use - will be _much_ nicer with Pirate
  def options[A](parser: OptionParser[A])(in: A => (ExtractOutput => ExtractOutput) => A): OptionParser[A] = {
    parser.opt[(String, String)]('o', "output")  action { (x, c) => in(c)(f => f.copy(formats = x :: f.formats)) }  unbounded() text
      "FORMAT=PATH Path to store output data. Supported formats [dense:psv, dense:csv, dense:tsv, sparse:psv, sparse:csv, sparse:tsv]."
    parser.opt[String]("missing-value")          action { (x, c) => in(c)(_.copy(missing = x)) } text
      "Value to use for missing values in output file, default 'NA'."
    parser
  }

  def parse(conf: IvoryConfiguration, output: ExtractOutput): RIO[OutputFormats] = for {
    out1 <- RIO.fromDisjunction[List[(OutputFormat, String)]](output.formats.traverseU {
      case (format, path) => OutputFormat.fromString(format).map(_ -> path)
        .toRightDisjunction(\&/.This(s"Unsupported format $format"): \&/[String, Throwable])
    })
    out2 <- RIO.fromDisjunctionString[List[(OutputFormat, OutputDataset)]](out1.traverseU({
      case (format, path) =>
        // This is a hack to ensure that paths that don't specify a scheme get defaulted to HdfsLocation. See `IvoryLocation.parseUri` vs `Location.fromUri`
        IvoryLocation.parseUri(path, conf).map(s => format -> OutputDataset(s.location))
    }))
  } yield OutputFormats(out2, output.missing)
}
