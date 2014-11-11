package com.ambiata.ivory.operation.extraction.snapshot

import argonaut._, Argonaut._
import com.ambiata.ivory.core._
import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import scalaz._, Scalaz._

case class SnapshotStats(version: IvoryVersion, datetime: DateTime, factCount: Map[FeatureId, Long])

/**
 * Stores the stats results of a snapshot as JSON, which can be useful for calculating skew and other such things.
 *
 * {{{
 *   {
 *      "version" : "1.0-20141105062749-226a694",
 *      "datetime" : "2014-10-09T16:36:00+0000"
 *      "fact_count" : {
 *         "my:feature1": "5",
 *         "my:feature2": "100"
 *      }
 *   }
 * }}}
 */
object SnapshotStats {

  def key(snapshotId: SnapshotId): Key =
    Repository.snapshot(snapshotId) / KeyName.unsafe(".stats")

  def load(repository: Repository, snapshotId: SnapshotId): ResultTIO[SnapshotStats] =
    repository.store.utf8.read(key(snapshotId)) >>=
      (json => ResultT.fromDisjunctionString(fromJson(json)))

  def save(repository: Repository, snapshotId: SnapshotId, stats: SnapshotStats): ResultTIO[Unit] =
    repository.store.utf8.write(key(snapshotId), toJson(stats))

  def toJson(stats: SnapshotStats): String =
    stats.asJson.spaces2

  def fromJson(stats: String): String \/ SnapshotStats =
    Parse.decodeEither[SnapshotStats](stats)

  implicit def SnapshotStatsEqual: Equal[SnapshotStats] =
    Equal.equalA[SnapshotStats]

  implicit def SnapshotStatsCodecJson: CodecJson[SnapshotStats] = CodecJson(
    stats => Json(
      "version" := stats.version,
      "datetime" := stats.datetime,
      "fact_count" := jObject(JsonObject.from(stats.factCount.map {
        case (fid, count) => fid.toString := count
      }.toList))
    ),
    c => (
      (c --\ "version").as[IvoryVersion] |@|
      (c --\ "datetime").as[DateTime] |@|
      // It would be handy if we could summon Map[FeatureId, Long]] directly
      (c --\ "fact_count").as[Map[String, Long]].flatMap[Map[FeatureId, Long]](m => m.toList.traverseU {
        case (fid, count) => FeatureId.parse(fid).map(_ -> count)
      }.fold(DecodeResult.fail(_, c.history), m => DecodeResult.ok(m.toMap)))
    )(SnapshotStats.apply)
  )
}
