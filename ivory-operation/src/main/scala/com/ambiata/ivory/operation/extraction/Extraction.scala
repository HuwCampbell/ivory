package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy.SnapshotMeta

import scalaz.Scalaz._

/**
 * WARNING: A short-term structure to represent the different kinds of extractions that are supported.
 *
 * This is currently required to ensure we can:
 *
 * 1. Use the correct dictionary for a given snapshot
 * 2. Squash data from snapshot before pivoting
 */
trait ExtractionInput
case class SnapshotExtract(meta: SnapshotMeta) extends ExtractionInput
case class ChordExtract(input: ReferenceIO) extends ExtractionInput

object Extraction {

  def extract(formats: OutputFormats, input: ExtractionInput): IvoryTIO[Unit] = IvoryT.fromResultTIO(repository =>
    formats.outputs.traverse {
      case (PivotFormat, output) =>
        println(s"Pivoting extracted file from '$input' to '${output.path}'")
        input match {
          case SnapshotExtract(meta)    =>
            Pivot.createPivotFromSnapshot(repository, output, formats.delim, formats.missingValue, meta)
          case ChordExtract(chordInput) =>
            Pivot.createPivot(repository, chordInput, output, formats.delim, formats.missingValue)
        }
    }.void
  )
}
