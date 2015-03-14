package com.ambiata.ivory.cli

import com.ambiata.ivory.core._
import com.ambiata.mundane.io.{Bytes, BytesQuantity}

import org.joda.time.{LocalDate, DateTimeZone}

import pirate._

import scalaz._, Scalaz._

object PirateReaders {

  implicit val FactsetIdRead: Read[FactsetId] =
    Read.eitherRead(s => FactsetId.parse(s).toRightDisjunction(s"'${s}' is not a valid factset-id."))

  implicit val SnapshotIdRead: Read[SnapshotId] =
    Read.eitherRead(s => SnapshotId.parse(s).toRightDisjunction(s"'${s}' is not a valid snapshot-id."))

  implicit val StrategyFlagRead: Read[StrategyFlag] =
    Read.eitherRead(s => StrategyFlag.fromString(s).toRightDisjunction(s"'${s}' is not a valid strategy."))

  implicit val DateTimeZoneRead: Read[DateTimeZone] =
    Read.tryRead(s => DateTimeZone.forID(s), "DATETIMEZONE")

  implicit val BytesQuantityRead: Read[BytesQuantity] =
    Read.of[Long].map(Bytes.apply)

  implicit val DateRead: Read[Date] =
    Read.tryRead(s => LocalDate.parse(s), "DATE").map(Date.fromLocalDate)
}
