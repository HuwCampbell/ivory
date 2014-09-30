package com.ambiata.ivory.operation.extraction

import org.apache.commons.logging.LogFactory
import scalaz.{Store => _, _}, Scalaz._
import org.apache.hadoop.io.compress._
import com.ambiata.notion.core._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.storage.legacy.{SnapshotMeta, FeatureStoreSnapshot}
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._, Metadata._

/**
 * A Chord is the extraction of feature values for some entities at some dates
 * 
 * Use the latest snapshot (if available) to get the latest values
 */
object Chord {
  private implicit val logger = LogFactory.getLog("ivory.operation.Chord")

  type PrioritizedFact = (Priority, Fact)

  /**
   * Create a chord from a list of entities
   * If takeSnapshot = true, take a snapshot first, otherwise use the latest available snapshot
   *
   * Returns a newly created [[FilePath]] to the chord in thrift format, which can be fed into other jobs.
   * Consumers of this method should delete the returned path when finished with the result.
   */
  def createChord(repository: Repository, entitiesLocation: IvoryLocation, takeSnapshot: Boolean): ResultTIO[FilePath] = for {
    _                   <- checkThat(repository, repository.isInstanceOf[HdfsRepository], "Chord only works on HDFS repositories at this stage.")
    entities            <- Entities.readEntitiesFrom(entitiesLocation)
    _                   <- logInfo(s"Earliest date in chord file is '${entities.earliestDate}'")
    _                   <- logInfo(s"Latest date in chord file is '${entities.latestDate}'")
    store               <- Metadata.latestFeatureStoreOrFail(repository)
    snapshot            <- if (takeSnapshot) Snapshot.takeSnapshot(repository, entities.earliestDate, incremental = true).map(Option.apply)
                           else              SnapshotMeta.latestSnapshot(repository, entities.earliestDate)
    out                 <- runChord(repository, store, entities, snapshot)
  } yield out

  /**
   * Run the chord extraction on Hdfs, returning the [[FilePath]] where the chord was written to.
   */
  def runChord(repository: Repository, store: FeatureStore, entities: Entities, incremental: Option[SnapshotMeta]): ResultTIO[FilePath] = {
    val outputPath = FilePath("tmp") </> java.util.UUID.randomUUID.toString
    for {
      hr                   <- downcast[Repository, HdfsRepository](repository, "Chord only works on HDFS repositories at this stage.")
      featureStoreSnapshot <- incremental.traverseU(meta => FeatureStoreSnapshot.fromSnapshotMeta(repository)(meta))
      dictionary           <- latestDictionaryFromIvory(repository)
      factsetGlobs         <- calculateGlobs(repository, store, entities.latestDate, featureStoreSnapshot)
      _                    <- job(repository, dictionary, factsetGlobs, outputPath, entities, featureStoreSnapshot, hr.codec).run(hr.configuration)
    } yield outputPath
  }

  def calculateGlobs(repository: Repository, featureStore: FeatureStore, latestDate: Date,
                     featureStoreSnapshot: Option[FeatureStoreSnapshot]): ResultTIO[List[Prioritized[FactsetGlob]]] =
    featureStoreSnapshot.cata(snapshot => for {
      oldGlobs    <- FeatureStoreGlob.between(repository, snapshot.store, snapshot.date, latestDate).map(_.globs)
      newFactsets  = featureStore diff snapshot.store
      _            = println(s"Reading factsets up to '$latestDate'\n${newFactsets.factsets}")
      newGlobs    <- FeatureStoreGlob.before(repository, newFactsets, latestDate).map(_.globs)
    } yield oldGlobs ++ newGlobs, FeatureStoreGlob.before(repository, featureStore, latestDate).map(_.globs))

  /**
   * create a new chord as a Map-Reduce job
   */
  def job(repository: Repository, dictionary: Dictionary, factsetsGlobs: List[Prioritized[FactsetGlob]],
          outputPath: FilePath, entities: Entities, snapshot: Option[FeatureStoreSnapshot],
          codec: Option[CompressionCodec]): Hdfs[Unit] = for {
    conf    <- Hdfs.configuration
    incrPath = snapshot.map(snap => repository.snapshot(snap.snapshotId).toHdfs)
    paths    =  factsetsGlobs.flatMap(_.value.paths.map(_.toHdfs)) ++ incrPath.toList
    size    <- paths.traverse(Hdfs.size).map(_.sum)
    _       <- Hdfs.log(s"Total input size: $size")
    reducers =  size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
    _       <- Hdfs.log(s"Number of reducers: $reducers")
    outPath  = (repository.root </> outputPath).toHdfs
    _       <- Hdfs.safe(ChordJob.run(conf, reducers.toInt, factsetsGlobs, outPath, entities, dictionary, incrPath, codec))
  } yield ()
}
