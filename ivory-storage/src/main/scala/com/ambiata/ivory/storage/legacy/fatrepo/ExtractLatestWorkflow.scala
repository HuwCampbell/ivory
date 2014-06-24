package com.ambiata.ivory.storage.legacy.fatrepo

import scalaz._, Scalaz._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.LocalDate
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.data._
import com.ambiata.ivory.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.mundane.io._

/**
 * This workflow is designed to extract the latest features from a feature store
 *
 * Steps:
 * 1. Find the latest version of a feature store and dictionary.
 *    - This will get a listing of all feature stores and dictionaries, ordering them by name, and taking the last (latest)
 * 2. Extract the most recent version of every feature for every entity.
 *    - Run snapshot app with discovered store and dictionary
 *    - Store in sparse row thrift format
 */
object ExtractLatestWorkflow {

  type FeatureStoreName = String
  type DictionaryName = String
  type Incremental = Option[(Path, SnapshotMeta)]
  type Extractor = (HdfsRepository, FeatureStoreName, DictionaryName, Date, Path, Incremental) => ScoobiAction[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.ExtractLatestWorkflow")

  def onHdfs(repoPath: Path, extractor: Extractor, date: Date, incremental: Boolean): ScoobiAction[(String, String, Path)] = {
    for {
      repo   <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
      store  <- ScoobiAction.fromHdfs(latestStore(repo))
      dname  <- ScoobiAction.fromHdfs(latestDictionary(repo))
      incr   <- ScoobiAction.fromHdfs(if(incremental) SnapshotMeta.latest(repo.snapshots.toHdfs, date) else Hdfs.ok(None))
      output <- incr.collect({ case (p, sm) if sm.date == date && sm.store == store =>
                  logger.info(s"Not running snapshot as already have a snapshot for '${date.hyphenated}' and '${store}'")
                  ScoobiAction.value(p)
                }).getOrElse(for {
                  out <- ScoobiAction.fromHdfs(outputDirectory(repo))
                  _    = logger.info(s"""
                                      | Running extractor on:
                                      | 
                                      | Repository     : ${repo.root.path}
                                      | Feature Store  : ${store}
                                      | Dictionary     : ${dname}
                                      | Date           : ${date.hyphenated}
                                      | Output         : ${out}
                                      | Incremental    : ${incr}
                                      |
                                      """.stripMargin)
                  _   <- extractor(repo, store, dname, date, out, incr)
                } yield out)
    } yield (store, dname, output)
  }

  def latestStore(repo: HdfsRepository): Hdfs[String] = for {
    _         <- Hdfs.value(logger.info(s"Finding latest feature store in the '${repo.root.path}' repository."))
    latestOpt <- latest(repo.stores.toHdfs)
    latest    <- latestOpt.map(Hdfs.ok(_)).getOrElse(Hdfs.fail(s"There are no feature stores"))
    _          = logger.info(s"Latest feature store is '${latest}'")
  } yield latest

  def latestDictionary(repo: HdfsRepository): Hdfs[String] = for {
    _         <- Hdfs.value(logger.info(s"Finding latest dictionary in the '${repo.root.path}' repository."))
    latestOpt <- latest(repo.dictionaries.toHdfs)
    latest    <- latestOpt.map(Hdfs.ok(_)).getOrElse(Hdfs.fail(s"There are no dictionaries'"))
    _          = logger.info(s"Latest dictionary is '${latest}'")
  } yield latest

  def latest(path: Path): Hdfs[Option[String]] =
    Hdfs.globPaths(path).map(_.map(_.getName).sorted(SOrdering[String].reverse).headOption)

  def latestIdentifier(base: Path): Hdfs[Option[Identifier]] =
    Hdfs.globPaths(base).map(_.flatMap(p => Identifier.parse(p.getName)).sorted(Identifier.IdentifierOrdering.reverse).headOption)

  def outputDirectory(repo: HdfsRepository): Hdfs[Path] = for {
    _ <- Hdfs.mkdir(repo.snapshots.toHdfs)
    _ <- Hdfs.value(logger.info(s"Finding snapshot output dir in '${repo.root.path}' repository."))
    l <- latestIdentifier(repo.snapshots.toHdfs)
    _  = logger.info(s"Latest snapshot output id is '${l}'")
    r <- Hdfs.mkdirWithRetry(new Path(repo.snapshots.toHdfs, l.getOrElse(Identifier.initial).render), prev => Identifier.parse(prev).flatMap(_.next.map(_.render)))
    p <- r.map(Hdfs.value).getOrElse(Hdfs.fail(s"Can not create output dir under ${repo.snapshots}"))
    _  = logger.info(s"New snapshot output dir is '${p}'")
  } yield p
}
