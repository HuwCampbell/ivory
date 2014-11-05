package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy.FeatureStoreSnapshot
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.io.compress._
import scalaz.{Store => _, _}, Scalaz._, effect.IO

/**
 * A Chord is the extraction of feature values for some entities at some dates
 *
 * Use the latest snapshot (if available) to get the latest values
 */
object Chord {

  type PrioritizedFact = (Priority, Fact)

  /**
   * Create a chord from a list of entities
   * If takeSnapshot = true, take a snapshot first, otherwise use the latest available snapshot
   *
   * Returns a newly created [[Key]] to the chord in thrift format, which can be fed into other jobs.
   * Consumers of this method should delete the returned path when finished with the result.
   */
  def createChord(repository: Repository, entitiesLocation: IvoryLocation, takeSnapshot: Boolean, windowing: Boolean): ResultTIO[(Key, Dictionary)] = for {
    _                   <- checkThat(repository, repository.isInstanceOf[HdfsRepository], "Chord only works on HDFS repositories at this stage.")
    _                    = if (windowing) NotImplemented.chordWindow()
    entities            <- Entities.readEntitiesFrom(entitiesLocation)
    _                    = println(s"Earliest date in chord file is '${entities.earliestDate}'")
    _                    = println(s"Latest date in chord file is '${entities.latestDate}'")
    store               <- Metadata.latestFeatureStoreOrFail(repository)
    snapshot            <- if (takeSnapshot) Snapshots.takeSnapshot(repository, entities.earliestDate).map(_.manifest.pure[Option])
                           else              SnapshotManifest.latestSnapshot(repository, entities.earliestDate).run
    out                 <- runChord(repository, store, entities, snapshot, windowing)
  } yield out

  /**
   * Run the chord extraction on Hdfs, returning the [[Key]] where the chord was written to.
   */
  def runChord(repository: Repository, store: FeatureStore, entities: Entities, incremental: Option[SnapshotManifest],
               windowing: Boolean): ResultTIO[(Key, Dictionary)] = {
    for {
      featureStoreSnapshot <- incremental.traverseU(SnapshotManifest.featureStoreSnapshot(repository, _))
      dictionary           <- latestDictionaryFromIvory(repository)
      factsetGlobs         <- calculateGlobs(repository, store, entities.latestDate, featureStoreSnapshot)
      outputPath           <- Repository.tmpDir(repository)

      /* DO NOT MOVE CODE BELOW HERE, NOTHING BESIDES THIS JOB CALL SHOULD MAKE HDFS ASSUMPTIONS. */
      hr                   <- repository.asHdfsRepository[IO]
      _                    <- job(hr, dictionary, factsetGlobs, outputPath, entities, featureStoreSnapshot, hr.codec, windowing).run(hr.configuration)
    } yield (outputPath, dictionary)
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
  def job(repository: HdfsRepository, dictionary: Dictionary, factsetsGlobs: List[Prioritized[FactsetGlob]],
          outputPath: Key, entities: Entities, snapshot: Option[FeatureStoreSnapshot],
          codec: Option[CompressionCodec], windowing: Boolean): Hdfs[Unit] = for {
    conf    <- Hdfs.configuration
    incrPath = snapshot.map(snap => repository.toIvoryLocation(Repository.snapshot(snap.snapshotId)).toHdfsPath)
    paths    =  factsetsGlobs.flatMap(_.value.keys.map(key => repository.toIvoryLocation(key).toHdfsPath)) ++ incrPath.toList
    size    <- paths.traverse(Hdfs.size).map(_.sum)
    _       <- Hdfs.log(s"Total input size: $size")
    reducers =  size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
    _       <- Hdfs.log(s"Number of reducers: $reducers")
    outPath  = repository.toIvoryLocation(outputPath).toHdfsPath
    _       <- Hdfs.fromResultTIO(ChordJob.run(repository, reducers.toInt, factsetsGlobs, outPath, entities, dictionary,
      incrPath, codec, windowing))
  } yield ()
}
