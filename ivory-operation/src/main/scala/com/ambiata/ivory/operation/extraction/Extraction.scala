package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy.SnapshotMeta
import com.ambiata.ivory.operation.extraction.output._

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
      case (DenseFormat(delim), output) =>
        println(s"Pivoting extracted file from '$input' to '${output.path}'")
        input match {
          case SnapshotExtract(meta)    =>
            PivotOutput.createPivotFromSnapshot(repository, output, delim, formats.missingValue, meta)
          case ChordExtract(chordInput) =>
            PivotOutput.createPivot(repository, chordInput, output, delim, formats.missingValue)
        }
      case (SparseFormat(delim), output) =>
        println(s"Storing extracted data '$input' to '${output.path}'")
        input match {
          case SnapshotExtract(meta)    =>
            EavOutput.extractFromSnapshot(repository, output, delim, formats.missingValue, meta)
          case ChordExtract(chordInput) =>
            EavOutput.extractFromChord(repository, chordInput, output, delim, formats.missingValue)
        }
    }.void
  )
}
