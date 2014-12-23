package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.extraction.squash.{SquashConfig, SquashJob}
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
   */
  def createChordWithSquash[A](repository: Repository, entitiesLocation: IvoryLocation, takeSnapshot: Boolean, config: SquashConfig,
                               outs: List[OutputDataset], cluster: Cluster)(f: (ShadowOutputDataset, Dictionary) => RIO[A]): RIO[A] = for {
    entities <- Entities.readEntitiesFrom(entitiesLocation)
    out      <- createChordRaw(repository, entities, takeSnapshot)
    hr       <- repository.asHdfsRepository[IO]
    job      <- SquashJob.initChordJob(hr.configuration, entities)
    // We always need to squash because the entities need to be rewritten, which is _only_ handled by squash
    // This can technically be optimized to do the entity rewriting in the reducer - see the Git history for an example
    a        <- SquashJob.squash(repository, out._2, out._1, config, outs, job, cluster)(f(_, out._2))
  } yield a

  /** Create the raw chord, which is now unusable without the squash step */
  def createChordRaw(repository: Repository, entities: Entities, takeSnapshot: Boolean): RIO[(ShadowOutputDataset, Dictionary)] = for {
    _                   <- checkThat(repository, repository.isInstanceOf[HdfsRepository], "Chord only works on HDFS repositories at this stage.")
    _                    = println(s"Earliest date in chord file is '${entities.earliestDate}'")
    _                    = println(s"Latest date in chord file is '${entities.latestDate}'")
    store               <- Metadata.latestFeatureStoreOrFail(repository)
    _                    = println(s"Latest store: ${store.id}")
    _                    = println(s"Are we taking a snapshot? ${takeSnapshot}")
    snapshot            <- if (takeSnapshot) Snapshots.takeSnapshot(repository, entities.earliestDate).map(_.meta.pure[Option])
                           else              SnapshotManifest.latestSnapshot(repository, entities.earliestDate).run
    _                    = println(s"Using snapshot: ${snapshot.map(_.snapshotId)}.")
    out                 <- runChord(repository, store, entities, snapshot)
  } yield out

  /**
   * Run the chord extraction on Hdfs, returning the [[Key]] where the chord was written to.
   */
  def runChord(repository: Repository, store: FeatureStore, entities: Entities,
               incremental: Option[SnapshotManifest]): RIO[(ShadowOutputDataset, Dictionary)] = {
    for {
      featureStoreSnapshot <- incremental.traverseU(SnapshotManifest.featureStoreSnapshot(repository, _))
      dictionary           <- latestDictionaryFromIvory(repository)
      _                     = println(s"Calculating globs using, store = ${store.id}, latest date = ${entities.latestDate}, snapshot: ${featureStoreSnapshot.map(_.snapshotId)}.")
      factsetGlobs         <- calculateGlobs(repository, store, entities.latestDate, featureStoreSnapshot)
      _                     = println(s"Calculated ${factsetGlobs.size} globs.")
      outputPath           <- Repository.tmpDir(repository)
      hdfsIvoryLocation    <- repository.toIvoryLocation(outputPath).asHdfsIvoryLocation[IO]
      out                   = ShadowOutputDataset.fromIvoryLocation(hdfsIvoryLocation)
      /* DO NOT MOVE CODE BELOW HERE, NOTHING BESIDES THIS JOB CALL SHOULD MAKE HDFS ASSUMPTIONS. */
      hr                   <- repository.asHdfsRepository[IO]
      _                    <- job(hr, dictionary, factsetGlobs, outputPath, entities, featureStoreSnapshot, hr.codec).run(hr.configuration)
    } yield (out, dictionary)
  }

  def calculateGlobs(repository: Repository, featureStore: FeatureStore, latestDate: Date,
                     featureStoreSnapshot: Option[FeatureStoreSnapshot]): RIO[List[Prioritized[FactsetGlob]]] =
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
          codec: Option[CompressionCodec]): Hdfs[Unit] = for {
    conf    <- Hdfs.configuration
    incrPath = snapshot.map(snap => repository.toIvoryLocation(Repository.snapshot(snap.snapshotId)).toHdfsPath)
    paths    =  factsetsGlobs.flatMap(_.value.keys.map(key => repository.toIvoryLocation(key).toHdfsPath)) ++ incrPath.toList
    size    <- paths.traverse(Hdfs.size).map(_.sum)
    _       <- Hdfs.log(s"Total input size: $size")
    reducers =  size.toBytes.value / 2.gb.toBytes.value + 1 // one reducer per 2GB of input
    _       <- Hdfs.log(s"Number of reducers: $reducers")
    outPath  = repository.toIvoryLocation(outputPath).toHdfsPath
    _       <- Hdfs.fromRIO(ChordJob.run(repository, reducers.toInt, factsetsGlobs, outPath, entities, dictionary,
      incrPath, codec))
  } yield ()
}
