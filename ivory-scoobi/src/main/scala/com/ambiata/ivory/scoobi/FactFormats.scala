package com.ambiata.ivory.scoobi

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.WireFormats.ShortWireFormat
import com.nicta.scoobi._, Scoobi._

trait FactFormats {
  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def FactWireFormat: WireFormat[Fact] = WireFormats.factWireFormat
  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def FactSeqSchema: SeqSchema[Fact] = SeqSchemas.factSeqSchema
  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def ThriftFactWireFormat: WireFormat[ThriftFact] = WireFormats.thriftFactWireFormat
  /** WARNING THIS MUST BE A DEF OR OR IT CAN TRIGGER CONCURRENCY ISSUES WITH SHARED THRIFT SERIALIZERS */
  implicit def ThriftFactSeqSchema: SeqSchema[ThriftFact] = SeqSchemas.thriftFactSeqSchema

  implicit def IdentifierWireFormat: WireFormat[Identifier] =
    implicitly[WireFormat[Int]].xmap(Identifier.unsafe, _.toInt)

  implicit def OldIdentifierWireFormat: WireFormat[OldIdentifier] =
    implicitly[WireFormat[Int]].xmap(OldIdentifier.unsafe, _.toInt)

  implicit def FactsetIdWireFormat: WireFormat[FactsetId] =
    implicitly[WireFormat[OldIdentifier]].xmap(FactsetId.apply, _.id)

  implicit def SnapshotIdWireFormat: WireFormat[SnapshotId] =
    implicitly[WireFormat[Identifier]].xmap(SnapshotId.apply, _.id)

  implicit def PriorityWireFormat: WireFormat[Priority] =
    implicitly[WireFormat[Short]].xmap(Priority.unsafe, _.toShort)

  implicit def FeatureIdWireFormat: WireFormat[FeatureId] = WireFormats.featureIdWireFormat

  implicit def ParseErrorWireFormat: WireFormat[ParseError] = WireFormats.parseErrorWireFormat
  implicit def ParseErrorSeqSchema: SeqSchema[ParseError] = SeqSchemas.parseErrorSeqSchema
}

object FactFormats extends FactFormats
