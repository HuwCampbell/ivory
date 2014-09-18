package com.ambiata.ivory.api

object Ivory
  extends com.ambiata.ivory.core.IvorySyntax
  with com.ambiata.ivory.scoobi.WireFormats
  with com.ambiata.ivory.scoobi.FactFormats {

  /**
   * Core ivory data types.
   */
  type Fact = com.ambiata.ivory.core.Fact
  val Fact = com.ambiata.ivory.core.Fact

  type Value = com.ambiata.ivory.core.Value
  val BooleanValue = com.ambiata.ivory.core.BooleanValue
  val IntValue = com.ambiata.ivory.core.IntValue
  val LongValue = com.ambiata.ivory.core.LongValue
  val DoubleValue = com.ambiata.ivory.core.DoubleValue
  val StringValue = com.ambiata.ivory.core.StringValue
  val TombstoneValue = com.ambiata.ivory.core.TombstoneValue

  type FactsetId = com.ambiata.ivory.core.FactsetId
  val FactsetId = com.ambiata.ivory.core.FactsetId

  type Priority = com.ambiata.ivory.core.Priority
  val Priority = com.ambiata.ivory.core.Priority

  type Dictionary = com.ambiata.ivory.core.Dictionary
  val Dictionary = com.ambiata.ivory.core.Dictionary

  type FeatureId = com.ambiata.ivory.core.FeatureId
  val FeatureId = com.ambiata.ivory.core.FeatureId

  type Definition = com.ambiata.ivory.core.Definition
  val Concrete = com.ambiata.ivory.core.Concrete
  val Virtual = com.ambiata.ivory.core.Virtual

  type ConcreteDefinition = com.ambiata.ivory.core.ConcreteDefinition
  val ConcreteDefinition = com.ambiata.ivory.core.ConcreteDefinition

  type Encoding = com.ambiata.ivory.core.Encoding
  val Encoding = com.ambiata.ivory.core.Encoding
  val BooleanEncoding = com.ambiata.ivory.core.BooleanEncoding
  val IntEncoding = com.ambiata.ivory.core.IntEncoding
  val LongEncoding = com.ambiata.ivory.core.LongEncoding
  val DoubleEncoding = com.ambiata.ivory.core.DoubleEncoding
  val StringEncoding = com.ambiata.ivory.core.StringEncoding

  // FIX rename??
  type Type = com.ambiata.ivory.core.Type
  val Type = com.ambiata.ivory.core.Type
  val NumericalType = com.ambiata.ivory.core.NumericalType
  val ContinuousType = com.ambiata.ivory.core.ContinuousType
  val CategoricalType = com.ambiata.ivory.core.CategoricalType
  val BinaryType = com.ambiata.ivory.core.BinaryType

  type FeatureStore = com.ambiata.ivory.core.FeatureStore
  val FeatureStore = com.ambiata.ivory.core.FeatureStore

  type Date = com.ambiata.ivory.core.Date
  val Date = com.ambiata.ivory.core.Date

  type DateTime = com.ambiata.ivory.core.DateTime
  val DateTime = com.ambiata.ivory.core.DateTime

  type Time = com.ambiata.ivory.core.Time
  val Time = com.ambiata.ivory.core.Time

  type ParseError = com.ambiata.ivory.core.ParseError
  val ParseError = com.ambiata.ivory.core.ParseError

  type Partition = com.ambiata.ivory.core.Partition
  val Partition = com.ambiata.ivory.core.Partition

  type Name = com.ambiata.ivory.core.Name
  val Name = com.ambiata.ivory.core.Name

  type Reference[F[_]] = com.ambiata.ivory.core.Reference[F]
  val Reference = com.ambiata.ivory.core.Reference
  type ReferenceIO = com.ambiata.ivory.core.ReferenceIO

  val Extraction = com.ambiata.ivory.operation.extraction.Extraction
  val SnapshotExtract = com.ambiata.ivory.operation.extraction.SnapshotExtract
  val ChordExtract = com.ambiata.ivory.operation.extraction.ChordExtract

  type OutputFormat = com.ambiata.ivory.operation.extraction.OutputFormat
  val OutputFormat = com.ambiata.ivory.operation.extraction.OutputFormat
  type OutputFormats = com.ambiata.ivory.operation.extraction.OutputFormats
  val OutputFormats = com.ambiata.ivory.operation.extraction.OutputFormats

  /**
   * Debug
   */
  val printErrors = com.ambiata.ivory.operation.display.PrintErrors.print _
  val printFacts = com.ambiata.ivory.operation.display.PrintFacts.print _
}
