package com.ambiata.ivory.storage.legacy.fatrepo

import scalaz.{Store => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.data._
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.storage.fact._
import com.ambiata.poacher.hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._

/**
 * This workflow is designed to extract the latest features from a feature store
 *
 * Steps:
 * 1. Find the latest version of a feature store.
 *    - This will get a listing of all feature stores, ordering them by name, and taking the last (latest)
 * 2. Extract the most recent version of every feature for every entity.
 *    - Run snapshot app with discovered store
 *    - Store in sparse row thrift format
 */
object ExtractLatestWorkflow {

  type Incremental = Option[(SnapshotId, SnapshotMeta)]
  type Extractor = (Repository, FeatureStoreId, Date, ReferenceIO, Incremental) => ResultTIO[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.ExtractLatestWorkflow")

  def onStore(repo: Repository, extractor: Extractor, date: Date, incremental: Boolean): ResultTIO[(FeatureStoreId, SnapshotId)] = {
    for {
      storeId <- latestStore(repo)
      incr    <- if(incremental) SnapshotMeta.latest(repo, date) else ResultT.ok[IO, Option[(SnapshotId, SnapshotMeta)]](None)
      snap    <- decideSnapshot(repo, date, storeId, incr)
      (skip, snapId) = snap
      output = repo.toReference(Repository.snapshot(snapId))
      _       <- if(skip) {
                  logger.info(s"Not running snapshot as already have a snapshot for '${date.hyphenated}' and '${storeId}'")
                  ResultT.ok[IO, Unit](())
                } else {
                  logger.info(s"""
                                 | Running extractor on:
                                 |
                                 | Repository     : ${repo.root.path}
                                 | Feature Store  : ${storeId.render}
                                 | Date           : ${date.hyphenated}
                                 | Output         : ${output}
                                 | Incremental    : ${incr}
                                 |
                                 """.stripMargin)
                  extractor(repo, storeId, date, output, incr)
                }
    } yield (storeId, snapId)
  }

  def decideSnapshot(repo: Repository, date: Date, storeId: FeatureStoreId, incr: Option[(SnapshotId, SnapshotMeta)]): ResultTIO[(Boolean, SnapshotId)] =
  incr.collect({ case (id, sm) if sm.date <= date && sm.store == storeId => for {
    store      <- Metadata.storeFromIvory(repo, storeId)
    partitions <- StoreGlob.between(repo, store, sm.date, date).map(_.flatMap(_.partitions))
    filtered = partitions.filter(_.date.isAfter(sm.date)) // TODO this should probably be in StoreGlob.between, but not sure what else it will affect
    skip       <- if(filtered.isEmpty) ResultT.ok[IO, (Boolean, SnapshotId)]((true, id)) else SnapshotMeta.allocateId(repo).map((false, _))
  } yield skip }).getOrElse(SnapshotMeta.allocateId(repo).map((false, _)))

  def latestStore(repo: Repository): ResultTIO[FeatureStoreId] = for {
    _         <- ResultT.ok[IO, Unit](logger.info(s"Finding latest feature store in the '${repo.root.path}' repository."))
    latestOpt <- Metadata.latestStoreId(repo)
    latest    <- ResultT.fromOption[IO, FeatureStoreId](latestOpt, s"There are no feature stores")
    _          = logger.info(s"Latest feature store is '${latest}'")
  } yield latest
}
