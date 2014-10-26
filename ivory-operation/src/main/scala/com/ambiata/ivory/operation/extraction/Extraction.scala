package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.squash.SquashConfig
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.metadata.SnapshotManifest
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
case class SnapshotExtract(meta: SnapshotManifest, squash: SquashConfig) extends ExtractionInput
case class ChordExtract(input: IvoryLocation) extends ExtractionInput

object Extraction {

  def extract(formats: OutputFormats, input: ExtractionInput): IvoryTIO[Unit] = IvoryT.fromResultTIO(repository =>
    formats.outputs.traverse {
      case (DenseFormat(delim), output) =>
        println(s"Pivoting extracted file from '$input' to '${output.show}'")
        input match {
          case SnapshotExtract(meta, conf)    =>
            PivotOutput.createPivotFromSnapshot(repository, output, delim, formats.missingValue, meta, conf)
          case ChordExtract(chordInput) =>
            PivotOutput.createPivot(repository, chordInput, output, delim, formats.missingValue)
        }
      case (SparseFormat(delim), output) =>
        println(s"Storing extracted data '$input' to '${output.show}'")
        input match {
          case SnapshotExtract(meta, conf)    =>
            EavOutput.extractFromSnapshot(repository, output, delim, formats.missingValue, meta, conf)
          case ChordExtract(chordInput) =>
            EavOutput.extractFromChord(repository, chordInput, output, delim, formats.missingValue)
        }
    }.void
  )
}
