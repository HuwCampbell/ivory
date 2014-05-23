package com.ambiata.ivory.extract

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.joda.time.LocalDate
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.parse._
import com.ambiata.mundane.time.DateTimex

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.core.thrift._
import com.ambiata.ivory.scoobi.WireFormats._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.scoobi.SeqSchemas._
import com.ambiata.ivory.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.validate.Validate
import com.ambiata.ivory.alien.hdfs._

case class HdfsSnapshot(repoPath: Path, store: String, dictName: String, entities: Option[Path], snapshot: LocalDate, outputPath: Path, errorPath: Path, incremental: Boolean) {
  import IvoryStorage._

  lazy val snapshotDate = Date.fromLocalDate(snapshot)

  def run: ScoobiAction[Unit] = for {
    r    <- ScoobiAction.scoobiConfiguration.map(sc => Repository.fromHdfsPath(repoPath.toString.toFilePath, sc))
    d    <- ScoobiAction.fromHdfs(dictionaryFromIvory(r, dictName))
    s    <- ScoobiAction.fromHdfs(storeFromIvory(r, store))
    es   <- ScoobiAction.fromHdfs(entities.traverseU(e => Hdfs.readLines(e)))
    incr <- ScoobiAction.fromHdfs(if(incremental) HdfsSnapshot.latestIncremental(r, snapshotDate) else Hdfs.ok(None))
    in   <- incr.traverseU(path => for {
        sm <- ScoobiAction.fromHdfs(SnapshotMeta.fromHdfs(new Path(path, SnapshotMeta.fname)))
        _   = println(s"Previous store was '${sm.store}'")
        _   = println(s"Previous date was '${sm.date.string("-")}'")
        s  <- ScoobiAction.fromHdfs(storeFromIvory(r, sm.store))
      } yield (path, s, sm))
    _    <- scoobiJob(r, d, s, es.map(_.toSet), in)
    _    <- ScoobiAction.fromHdfs(DictionaryTextStorage.DictionaryTextStorer(new Path(outputPath, ".dictionary")).store(d))
    _    <- ScoobiAction.fromHdfs(SnapshotMeta(snapshotDate, store).toHdfs(new Path(outputPath, SnapshotMeta.fname)))
  } yield ()

  def scoobiJob(repo: HdfsRepository, dict: Dictionary, store: FeatureStore, entities: Option[Set[String]], incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      // disable combiners as it's just overhead. The data is partitioned by date, so each mapper will have
      // only one date in it
      sc.disableCombiners

      lazy val factsetMap: Map[Priority, Factset] = {
        val exclude = incremental.toList.flatMap(_._2.factsets).map(_.set.name).toSet
        val base = store.factsets.filter(fs => !exclude.contains(fs.set.name)).map(fs => (fs.priority, fs.set)).toMap
        base + (Priority.Max -> Factset(HdfsSnapshot.SnapshotName))
      }

      HdfsSnapshot.readFacts(repo, store, snapshotDate, incremental).map(input => {

        val facts: DList[(Priority, Fact)] = input.map({
          case -\/(e) => sys.error("A critical error has occured, where we could not determine priority and namespace from partitioning: " + e)
          case \/-(v) => v
        }).collect({
          case (p, _, f) if f.date.isBefore(snapshotDate) && entities.map(_.contains(f.entity)).getOrElse(true) => (p, f)
        })

        /*
         * 1. group by entity and feature id
         * 2. take the minimum fact in the group using fact time then priority to determine order
         */
        val ord: Order[(Priority, Fact)] = Order.orderBy { case (p, f) => (-f.datetime.long, p) }
        val latest: DList[(Priority, Fact)] = facts.groupBy { case (p, f) => (f.entity, f.featureId.toString) }
                                                   .reduceValues(Reduction.minimum(ord))
                                                   .collect { case (_, (p, f)) if !f.isTombstone => (p, f) }

        val validated: DList[Fact] = latest.map({ case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(e => e + " - Factset " + factsetMap.get(p).getOrElse("Unknown, priority " + p))
        }).map({
          case -\/(e) => sys.error("A critical error has occurred, a value in ivory no longer matches the dictionary: " + e)
          case \/-(v) => v
        })

        persist(validated.valueToSequenceFile(outputPath.toString, overwrite = true).compressWith(new SnappyCodec))

        ()
      })
    }).flatten
}


object HdfsSnapshot {
  val SnapshotName: String = "ivory-incremental-snapshot"

  def takeSnapshot(repoPath: Path, errors: Path, date: LocalDate, incremental: Boolean): ScoobiAction[(String, String, Path)] =
    fatrepo.ExtractLatestWorkflow.onHdfs(repoPath, extractLatest(errors), date, incremental)

  def extractLatest(errorPath: Path)(repo: HdfsRepository, store: String, dictName: String, date: LocalDate, outputPath: Path, incremental: Boolean): ScoobiAction[Unit] = for {
    d  <- ScoobiAction.fromHdfs(IvoryStorage.dictionaryFromIvory(repo, dictName))
    _  <- HdfsSnapshot(repo.root.toHdfs, store, dictName, None, date, outputPath, errorPath, incremental).run
  } yield ()

  def readFacts(repo: HdfsRepository, store: FeatureStore, latestDate: Date, incremental: Option[(Path, FeatureStore, SnapshotMeta)]): ScoobiAction[DList[ParseError \/ (Priority, Factset, Fact)]] = {
    import IvoryStorage._
    incremental match {
      case None             => factsFromIvoryStore(repo, store)
      case Some((p, s, sm)) => for {
        o <- factsFromIvoryStoreBetween(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd = store --- s
        _  = println(s"Fully reading factsets '${sd.factsets}'")
        n <- factsFromIvoryStoreTo(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
      } yield o ++ n ++ valueFromSequenceFile[Fact](p.toString).map(fact => (Priority.Max, Factset(SnapshotName), fact).right[ParseError])
    }
  }

  def latestIncremental(repo: HdfsRepository, date: Date): Hdfs[Option[Path]] = for {
    paths <- Hdfs.globPaths(repo.snapshots.toHdfs)
    metas <- paths.traverse(p => {
      val snapmeta = new Path(p, SnapshotMeta.fname)
      Hdfs.exists(snapmeta).flatMap(e =>
        if(e) SnapshotMeta.fromHdfs(snapmeta).map[Option[(Path, SnapshotMeta)]](sm => Some((p, sm))) else Hdfs.value(None))
    }).map(_.flatten)
  } yield metas.filter(_._2.date.isBeforeOrEqual(date)).sortBy(_._2.date).map(_._1).lastOption
}

case class SnapshotMeta(date: Date, store: String) {

  def toHdfs(path: Path): Hdfs[Unit] =
    Hdfs.writeWith(path, os => Streams.write(os, stringLines))

  lazy val stringLines: String =
    date.string("-") + "\n" + store
}

object SnapshotMeta {

  val fname = ".snapmeta"

  def fromHdfs(path: Path): Hdfs[SnapshotMeta] = for {
    raw <- Hdfs.readWith(path, is => Streams.read(is))
    sm  <- Hdfs.fromValidation(parser.run(raw.lines.toList))
  } yield sm

  def parser: ListParser[SnapshotMeta] = {
    import ListParser._
    for {
      d <- localDate
      s <- string.nonempty
    } yield SnapshotMeta(Date.fromLocalDate(d), s)
  }
}
