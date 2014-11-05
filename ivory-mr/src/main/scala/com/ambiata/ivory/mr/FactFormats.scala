package com.ambiata.ivory.mr

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.core._
import com.ambiata.ivory.mr.WireFormats.ShortWireFormat
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
    implicitly[WireFormat[String]].xmap(x => Identifier.parse(x).getOrElse(sys.error("WireFormat invariant violated.")), _.render)

  implicit def FactsetIdWireFormat: WireFormat[FactsetId] =
    implicitly[WireFormat[Identifier]].xmap(FactsetId.apply, _.id)

  implicit def SnapshotIdWireFormat: WireFormat[SnapshotId] =
    implicitly[WireFormat[Identifier]].xmap(SnapshotId.apply, _.id)

  implicit def PriorityWireFormat: WireFormat[Priority] =
    implicitly[WireFormat[Short]].xmap(Priority.unsafe, _.toShort)

  implicit def FeatureIdWireFormat: WireFormat[FeatureId] = WireFormats.featureIdWireFormat

  implicit def ParseErrorWireFormat: WireFormat[ParseError] = WireFormats.parseErrorWireFormat

  implicit def ParseErrorSeqSchema: SeqSchema[ParseError] = SeqSchemas.parseErrorSeqSchema
}

object FactFormats extends FactFormats
