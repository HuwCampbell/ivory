package com.ambiata.ivory.cli.extract

import com.ambiata.ivory.api.Ivory.{OutputFormat, OutputFormats}
import com.ambiata.ivory.cli.ScoptReaders.charRead
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import scopt.OptionParser

import scalaz._, Scalaz._, scalaz.effect._

case class ExtractOutput(formats: List[(String, String)] = Nil, delim: Char = '|', tombstone: String = "NA")

object Extract {

  // This is pretty horrible just for simple re-use - will be _much_ nicer with Pirate
  def options[A](parser: OptionParser[A])(in: A => (ExtractOutput => ExtractOutput) => A): OptionParser[A] = {
    parser.opt[(String, String)]('o', "output")  action { (x, c) => in(c)(f => f.copy(formats = x :: f.formats)) }  unbounded() text
      "FORMAT=PATH Path to store pivot data. Supported formats [dense:psv]."
    parser.opt[Char]("delim")                    action { (x, c) => in(c)(_.copy(delim = x)) } text
      "Delimiter for pivot file, default '|'."
    parser.opt[String]("missing-value")              action { (x, c) => in(c)(_.copy(tombstone = x)) } text
      "Value to use for missing values in pivot file, default 'NA'."
    parser
  }

  def parse(conf: RepositoryConfiguration, output: ExtractOutput): ResultTIO[OutputFormats] = for {
    out1 <- ResultT.fromDisjunction[IO, List[(OutputFormat, String)]](output.formats.traverseU {
      case (format, path) => OutputFormat.fromString(format).map(_ -> path)
        .toRightDisjunction(\&/.This(s"Unsupported format $format"): \&/[String, Throwable])
    })
    out2 <- out1.traverseU {
      case (format, path) => Reference.fromUriResultTIO(path, conf).map(format ->)
    }
  } yield OutputFormats(out2, output.delim, output.tombstone)
}
