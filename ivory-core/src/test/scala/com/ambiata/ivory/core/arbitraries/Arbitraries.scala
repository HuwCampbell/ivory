package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import org.joda.time.DateTimeZone

import scalaz.{Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._


trait Arbitraries {
  implicit def BytesArbitrary: Arbitrary[Bytes] =
    Arbitrary(GenRepository.bytes)

  implicit def CommitArbitrary: Arbitrary[Commit] =
    Arbitrary(GenRepository.commit)

  implicit def CommitIdArbitrary: Arbitrary[CommitId] =
    Arbitrary(GenIdentifier.commit)

  implicit def CommitMetadataArbitrary: Arbitrary[CommitMetadata] =
    Arbitrary(GenRepository.commitMetadata)

  implicit def ConcreteDefinitionArbitrary: Arbitrary[ConcreteDefinition] =
    Arbitrary(GenDictionary.concrete)

  implicit def DatasetArbitrary: Arbitrary[Dataset] =
    Arbitrary(GenRepository.dataset)

  implicit def DatasetsArbitrary: Arbitrary[Datasets] =
    Arbitrary(GenRepository.datasets)

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(GenDate.date)

  implicit def DateTimeArbitrary: Arbitrary[DateTime] =
    Arbitrary(GenDate.dateTime)

  implicit def DateTimeZoneArbitrary: Arbitrary[DateTimeZone] =
    Arbitrary(GenDate.zone)

  implicit def DelimiterArbitrary: Arbitrary[Delimiter] =
    Arbitrary(GenFileFormat.delimiter)

  implicit def DictionaryArbitrary: Arbitrary[Dictionary] =
    Arbitrary(GenDictionary.dictionary)

  implicit def DictionaryIdArbitrary: Arbitrary[DictionaryId] =
    Arbitrary(GenIdentifier.dictionary)

  implicit def EncodingArbitrary: Arbitrary[Encoding] =
    Arbitrary(GenDictionary.encoding)

  implicit def FactArbitrary: Arbitrary[Fact] =
    Arbitrary(GenFact.fact)

  implicit def FactsetArbitrary: Arbitrary[Factset] =
    Arbitrary(GenRepository.factset)

  implicit def FactsetFormatArbitrary: Arbitrary[FactsetFormat] =
    Arbitrary(GenVersion.factset)

  implicit def FactsetIdArbitrary: Arbitrary[FactsetId] =
    Arbitrary(GenIdentifier.factset)

  implicit def FeatureIdArbitrary: Arbitrary[FeatureId] =
    Arbitrary(GenIdentifier.feature)

  implicit def FeatureIdMappingsArbitrary: Arbitrary[FeatureIdMappings] =
    Arbitrary(GenIdentifier.featureMappings)

  implicit def FeatureStoreArbitrary: Arbitrary[FeatureStore] =
    Arbitrary(GenRepository.store)

  implicit def FeatureStoreIdArbitrary: Arbitrary[FeatureStoreId] =
    Arbitrary(GenIdentifier.store)

  implicit def FeatureWindowArbitrary: Arbitrary[FeatureWindow] =
    Arbitrary(GenDictionary.featureWindow)

  implicit def FeatureWindowsArbitrary: Arbitrary[FeatureWindows] =
    Arbitrary(GenDictionary.featureWindows)

  implicit def FactStatisticsArbitrary: Arbitrary[FactStatistics] =
    Arbitrary(GenRepository.factStatistics)

  implicit def FileFormatArbitrary: Arbitrary[FileFormat] =
    Arbitrary(GenFileFormat.format)

  implicit def FormArbitrary: Arbitrary[Form] =
    Arbitrary(GenFileFormat.form)

  implicit def IdentifiedArbitrary[A: Arbitrary, B: Arbitrary]: Arbitrary[Identified[A, B]] =
    Arbitrary(GenIdentifier.identified)

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(GenIdentifier.identifier)

  implicit def IvoryFlagsArbitrary: Arbitrary[IvoryFlags] =
    Arbitrary(GenFlags.flags)

  implicit def IvoryVersionArbitrary: Arbitrary[IvoryVersion] =
    Arbitrary(GenIdentifier.version)

  implicit def ListEncodingArbitrary: Arbitrary[ListEncoding] =
    Arbitrary(GenDictionary.listEncoding)

  implicit def MetadataVersionArbitrary: Arbitrary[MetadataVersion] =
    Arbitrary(GenVersion.metadata)

  implicit def ModeArbitrary: Arbitrary[Mode] =
    Arbitrary(GenDictionary.mode)

  implicit def NamespaceArbitrary: Arbitrary[Namespace] =
    Arbitrary(GenString.namespace)

  implicit def OutputFormatArbitrary: Arbitrary[OutputFormat] =
    Arbitrary(GenFileFormat.output)

  implicit def PartitionArbitrary: Arbitrary[Partition] =
    Arbitrary(GenRepository.partition)

  implicit def PrimitiveEncodingArbitrary: Arbitrary[PrimitiveEncoding] =
    Arbitrary(GenDictionary.primitiveEncoding)

  implicit def PrioritizedArbitrary[A: Arbitrary]: Arbitrary[Prioritized[A]] =
    Arbitrary((arbitrary[Priority] |@| arbitrary[A])(Prioritized.apply))

  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(Gen.choose(Priority.Min.toShort, Priority.Max.toShort).map(Priority.unsafe))

  implicit def RangeArbitrary[A: Arbitrary]: Arbitrary[Range[A]] =
    Arbitrary(GenDictionary.range[A])

  implicit def RangesArbitrary[A: Arbitrary: Equal]: Arbitrary[Ranges[A]] =
    Arbitrary(GenDictionary.ranges[A])

  implicit def RepositoryConfigArbitrary: Arbitrary[RepositoryConfig] =
    Arbitrary(GenRepository.repositoryConfig)

  implicit def RepositoryConfigArbitraryId: Arbitrary[RepositoryConfigId] =
    Arbitrary(GenIdentifier.repositoryConfigId)

  implicit def SizedArbitrary[A: Arbitrary]: Arbitrary[Sized[A]] =
    Arbitrary(GenRepository.sized(arbitrary[A]))

  implicit def SnapshotArbitrary: Arbitrary[Snapshot] =
    Arbitrary(GenRepository.snapshot)

  implicit def SnapshotFormatArbitrary: Arbitrary[SnapshotFormat] =
    Arbitrary(GenVersion.snapshot)

  implicit def SnapshotIdArbitrary: Arbitrary[SnapshotId] =
    Arbitrary(GenIdentifier.snapshot)

  implicit def StrategyFlagArbitrary: Arbitrary[StrategyFlag] =
    Arbitrary(GenFlags.plan)

  implicit def StructEncodingArbitrary: Arbitrary[StructEncoding] =
    Arbitrary(GenDictionary.structEncoding)

  implicit def SubEncodingArbitrary: Arbitrary[SubEncoding] =
    Arbitrary(GenDictionary.subEncoding)

  implicit def TextEncodingArbitrary: Arbitrary[TextEscaping] =
    Arbitrary(GenFileFormat.encoding)

  implicit def TimeArbitrary: Arbitrary[Time] =
    Arbitrary(GenDate.time)

  implicit def TypeArbitrary: Arbitrary[Type] =
    Arbitrary(GenDictionary.type_)

  implicit def PrimitiveValueArbitrary: Arbitrary[PrimitiveValue] =
    Arbitrary(GenValue.primitiveValue)

  implicit def WindowArbitrary: Arbitrary[Window] =
    Arbitrary(GenDictionary.window)
}

object Arbitraries extends Arbitraries
