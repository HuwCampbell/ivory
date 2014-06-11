package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.LocalDate
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.SeqSchemas._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.alien.hdfs._
import MemoryConversions._

case class HdfsSnapshot(repoPath: Path, store: String, entities: Option[Path], snapshot: Date, outputPath: Path, incremental: Option[(Path, SnapshotMeta)], codec: Option[CompressionCodec]) {
  import IvoryStorage._

  def run: ScoobiAction[Unit] = ScoobiAction.scoobiConfiguration.flatMap(sc => ScoobiAction.fromHdfs(for {
    r    <- Repository.fromHdfsPath(repoPath.toString.toFilePath, sc).pure[Hdfs]
    d    <- Hdfs.fromResultTIO { dictionaryFromIvory(r) }
    s    <- storeFromIvory(r, store)
    es   <- entities.traverseU(e => Hdfs.readLines(e))
    in   <- incremental.traverseU({ case (path, sm) => for {
              _ <- Hdfs.value({
                     println(s"Previous store was '${sm.store}'")
                     println(s"Previous date was '${sm.date.string("-")}'")
                   })
              s <- storeFromIvory(r, sm.store)
            } yield (path, s, sm) })
    _    <- job(r, s, in, codec)
    _    <- DictionaryTextStorageV2.toHdfs(new Path(outputPath, ".dictionary"), d)
    _    <- SnapshotMeta(snapshot, store).toHdfs(new Path(outputPath, SnapshotMeta.fname))
  } yield ()))

  def job(repo: HdfsRepository, store: FeatureStore, incremental: Option[(Path, FeatureStore, SnapshotMeta)], codec: Option[CompressionCodec]): Hdfs[Unit] = for {
    conf  <- Hdfs.configuration
    globs <- HdfsSnapshot.storePaths(repo, store, snapshot, incremental)
    paths  = globs.flatMap(_.partitions.map(p => new Path(p.path))) ++ incremental.map(_._1).toList
    size  <- paths.traverse(Hdfs.size).map(_.sum)
    _     = println(s"Total input size: ${size}")
    reducers = size.toBytes.value / 768.mb.toBytes.value + 1 // one reducer per 768MB of input
    _     = println(s"Number of reducers: ${reducers}")
    _     <- Hdfs.safe(SnapshotJob.run(conf, reducers.toInt, snapshot, globs, outputPath, incremental.map(_._1), codec))
  } yield ()
}

object HdfsSnapshot {
  val SnapshotName: String = "ivory-incremental-snapshot"

  def takeSnapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[(String, Path)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repoPath, extractLatest(codec), date, incremental)

  /* This is exposed through the external API */
  def snapshot(repoPath: Path, date: Date, incremental: Boolean, codec: Option[CompressionCodec]): ScoobiAction[Path] =
    takeSnapshot(repoPath, date, incremental, codec).map(_._2)

  def extractLatest(codec: Option[CompressionCodec])(repo: HdfsRepository, store: String, date: Date, outputPath: Path, incremental: Option[(Path, SnapshotMeta)]): ScoobiAction[Unit] =
    HdfsSnapshot(repo.root.toHdfs, store, None, date, outputPath, incremental, codec).run

  def readFacts(repo: HdfsRepository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] = {
    import IvoryStorage._
    incremental match {
      case None =>
        factsFromIvoryStoreTo(repo, store, latestDate)
      case Some((p, s, sm)) => for {
        sc <- ScoobiAction.scoobiConfiguration
        o  <- factsFromIvoryStoreBetween(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd  = store --- s
        _   = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        n  <- factsFromIvoryStoreTo(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
      } yield o ++ n ++ FlatFactThriftStorageV1.FlatFactThriftLoader(p.toString).loadScoobi(sc).map(_.map((Priority.Max, Factset(SnapshotName), _)))
    }
  }

  def storePaths(repo: HdfsRepository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): Hdfs[List[FactsetGlob]] = Hdfs.fromResultTIO({
    incremental match {
      case None =>
        StoreGlob.before(repo, store, latestDate)
      case Some((p, s, sm)) => for {
        o <- StoreGlob.between(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd = store --- s
        _  = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        n <- StoreGlob.before(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
      } yield FactsetGlob.groupByVersion(o ++ n)
    }
  })
}
