package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._

import scalaz._, Scalaz._

object InputFormat {

  def fromString(input: String): String \/ (FileFormat, Option[Namespace], String) =
    (input.split("=", 2) match {
      case Array(formatAndNs, path) => formatAndNs.split("\\|", 2) match {
        case Array(format, ns) =>
          OutputFormat.fromString(format).toRightDisjunction(s"Invalid format $format")
            .flatMap(f => Namespace.nameFromString(ns).toRightDisjunction(s"Invalid namespace $ns")
            .map(n => (f, Some(n), path)))
        case Array(format) =>
          OutputFormat.fromString(format).toRightDisjunction(s"Invalid format $format").map(f => (f, None, path))
      }
      case _ =>
        "Invalid input, missing format".left
    }).flatMap { x =>
      if (x._1.form != Form.Sparse) s"Only sparse supported for ingest: ${x._1.form}".left
      else (x._1.format, x._2, x._3).right
    }

  def render(format: FileFormat, ns: Option[Namespace], path: String): String =
    "sparse:" + format.render + ns.cata("|" + _.name, "") + "=" + path
}
