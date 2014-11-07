package com.ambiata.ivory.core.arbitraries

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.gen._

import org.scalacheck._, Arbitrary._
import org.joda.time.DateTimeZone

import scalaz.{Name => _, Value => _, _}, Scalaz._
import scalaz.scalacheck.ScalaCheckBinding._


trait Arbitraries {
  implicit def CommitArbitrary: Arbitrary[Commit] =
    Arbitrary(GenRepository.commit)

  implicit def CommitIdArbitrary: Arbitrary[CommitId] =
    Arbitrary(GenIdentifier.commit)

  implicit def ConcreteDefinitionArbitrary: Arbitrary[ConcreteDefinition] =
    Arbitrary(GenDictionary.concrete)

  implicit def DateArbitrary: Arbitrary[Date] =
    Arbitrary(GenDate.date)

  implicit def DateTimeArbitrary: Arbitrary[DateTime] =
    Arbitrary(GenDate.dateTime)

  implicit def DateTimeZoneArbitrary: Arbitrary[DateTimeZone] =
    Arbitrary(GenDate.zone)

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

  implicit def FactsetIdArbitrary: Arbitrary[FactsetId] =
    Arbitrary(GenIdentifier.factset)

  implicit def FeatureIdArbitrary: Arbitrary[FeatureId] =
    Arbitrary(GenIdentifier.feature)

  implicit def FeatureStoreArbitrary: Arbitrary[FeatureStore] =
    Arbitrary(GenRepository.store)

  implicit def FeatureStoreIdArbitrary: Arbitrary[FeatureStoreId] =
    Arbitrary(GenIdentifier.store)

  implicit def IdentifierArbitrary: Arbitrary[Identifier] =
    Arbitrary(GenIdentifier.identifier)

  implicit def ListEncodingArbitrary: Arbitrary[ListEncoding] =
    Arbitrary(GenDictionary.listEncoding)

  implicit def ModeArbitrary: Arbitrary[Mode] =
    Arbitrary(GenDictionary.mode)

  implicit def NameArbitrary: Arbitrary[Name] =
    Arbitrary(GenString.name)

  implicit def PartitionArbitrary: Arbitrary[Partition] =
    Arbitrary(GenRepository.partition)

  implicit def PartitionsArbitrary: Arbitrary[Partitions] =
    Arbitrary(GenRepository.partitions)

  implicit def PrimitiveEncodingArbitrary: Arbitrary[PrimitiveEncoding] =
    Arbitrary(GenDictionary.primitiveEncoding)

  implicit def PrioritizedArbitrary[A: Arbitrary]: Arbitrary[Prioritized[A]] =
    Arbitrary((arbitrary[Priority] |@| arbitrary[A])(Prioritized.apply))

  implicit def PriorityArbitrary: Arbitrary[Priority] =
    Arbitrary(Gen.choose(Priority.Min.toShort, Priority.Max.toShort).map(Priority.unsafe))

  implicit def SnapshotIdArbitrary: Arbitrary[SnapshotId] =
    Arbitrary(GenIdentifier.snapshot)

  implicit def StructEncodingArbitrary: Arbitrary[StructEncoding] =
    Arbitrary(GenDictionary.structEncoding)

  implicit def SubEncodingArbitrary: Arbitrary[SubEncoding] =
    Arbitrary(GenDictionary.subEncoding)

  implicit def TimeArbitrary: Arbitrary[Time] =
    Arbitrary(GenDate.time)

  implicit def TypeArbitrary: Arbitrary[Type] =
    Arbitrary(GenDictionary.type_)

  implicit def ValueArbitrary: Arbitrary[Value] =
    Arbitrary(GenValue.value)

  implicit def WindowArbitrary: Arbitrary[Window] =
    Arbitrary(GenDictionary.window)
}

object Arbitraries extends Arbitraries
