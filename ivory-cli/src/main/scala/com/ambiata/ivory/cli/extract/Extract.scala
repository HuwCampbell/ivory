package com.ambiata.ivory.cli.extract

import com.ambiata.ivory.api.Ivory._
import com.ambiata.ivory.cli.read.Assign
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._

import pirate._, Pirate._

import scalaz._, Scalaz._

case class ExtractOutput(formats: List[(String, String)], missing: String)

object Extract {

  def parseOutput: Parse[ExtractOutput] = ExtractOutput |*| (
    flag[Assign[String, String]](both('o', "output"), description("""
      |FORMAT=PATH Path to store output data.
      |Supported formats [dense:psv, dense:csv, dense:tsv, sparse:psv, sparse:csv, sparse:tsv].
      |""".stripMargin)).map(_.tuple).many
  , flag[String](long("missing-value"),
    description("Value to use for missing values in output file, default 'NA'.")).default("NA")
  )

  def parseSquashConfig: Parse[SquashConfig] =
    flag[Int](long("sample-rate"), description("""
      |Every X number of facts will be sampled when calculating virtual results. Defaults to 1,000,000.
      |WARNING: Decreasing this number will degrade performance.
      |""".stripMargin)
    ).map(SquashConfig.apply).default(SquashConfig.default)

  def parse(conf: IvoryConfiguration, output: ExtractOutput): RIO[OutputFormats] = for {
    out1 <- RIO.fromDisjunction[List[(OutputFormat, String)]](output.formats.traverseU {
      case (format, path) => OutputFormat.fromString(format).map(_ -> path)
        .toRightDisjunction(\&/.This(s"Unsupported format $format"): \&/[String, Throwable])
    })
    out2 <- RIO.fromDisjunctionString[List[(OutputFormat, OutputDataset)]](out1.traverseU({
      case (format, path) =>
        // This is a hack to ensure that paths that don't specify a scheme get defaulted to HdfsLocation.
        // See `IvoryLocation.parseUri` vs `Location.fromUri`
        IvoryLocation.parseUri(path, conf).map(s => format -> OutputDataset(s.location))
    }))
  } yield OutputFormats(out2, output.missing)
}
