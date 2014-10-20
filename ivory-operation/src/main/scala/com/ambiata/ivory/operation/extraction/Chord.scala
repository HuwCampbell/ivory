package com.ambiata.ivory.operation.extraction

import java.util

import com.ambiata.ivory.storage.legacy.FlatFactThriftStorageV1.FlatFactThriftLoader
import com.nicta.scoobi.Scoobi._
import org.apache.commons.logging.LogFactory
import scalaz.{DList => _, Store => _, Value => _, _}, Scalaz._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.notion.core._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.metadata._, Metadata._
import Entities._
import com.ambiata.ivory.scoobi._
import scala.collection.JavaConversions._
import IvorySyntax._

/**
 * A Chord is the extraction of feature values for some entities at some dates
 *
 * Use the latest snapshot (if available) to get the latest values
 */
object Chord {
  private implicit val logger = LogFactory.getLog("ivory.operation.Snapshot")

  type PrioritizedFact = (Priority, Fact)

  /**
   * Create a chord from a list of entities
   * If takeSnapshot = true, take a snapshot first, otherwise use the latest available snapshot
   *
   * Returns a newly created [[FilePath]] to the chord in thrift format, which can be fed into other jobs.
   * Consumers of this method should delete the returned path when finished with the result.
   */
  def createChord(repository: Repository, entitiesLocation: IvoryLocation, takeSnapshot: Boolean): ResultTIO[Key] = for {
    _                   <- checkThat(repository, repository.isInstanceOf[HdfsRepository], "Chord only works on HDFS repositories at this stage.")
    entities            <- Entities.readEntitiesFrom(entitiesLocation)
    _                   <- logInfo(s"Earliest date in chord file is '${entities.earliestDate}'")
    _                   <- logInfo(s"Latest date in chord file is '${entities.latestDate}'")
    store               <- Metadata.latestFeatureStoreOrFail(repository)
    snapshot            <- if (takeSnapshot) Snapshot.takeSnapshot(repository, entities.earliestDate, incremental = true).map(Option.apply)
                           else              SnapshotMeta.latestSnapshot(repository, entities.earliestDate)
    out                 <- runChordOnHdfs(repository, store, entities, snapshot)
  } yield out

  /**
   * Run the chord extraction on Hdfs, returning the [[FilePath]] where the chord was written to.
   */
  def runChordOnHdfs(repository: Repository, store: FeatureStore, entities: Entities, incremental: Option[SnapshotMeta]): ResultTIO[Key] = {
    val chordKey = Repository.root / "tmp" / KeyName.fromUUID(java.util.UUID.randomUUID)
    val outputKey = Repository.root / "tmp" / KeyName.fromUUID(java.util.UUID.randomUUID)
    for {
      hr                   <- downcast[Repository, HdfsRepository](repository, "Chord only works on HDFS repositories at this stage.")
      _                    <- serialiseEntities(repository, entities, chordKey)
      featureStoreSnapshot <- incremental.traverseU(meta => FeatureStoreSnapshot.fromSnapshotMeta(repository)(meta))
      dictionary           <- latestDictionaryFromIvory(repository)
      _                    <- chordScoobiJob(hr, dictionary, store, chordKey, entities.latestDate, featureStoreSnapshot,
                                             hr.toIvoryLocation(outputKey).toHdfsPath, hr.codec).run(hr.scoobiConfiguration)
      // Delete the temporary chordRef - no longer needed
      _                    <- repository.store.delete(chordKey)
    } yield outputKey
  }

  /**
   * Persist facts which are the latest corresponding to a set of dates given for each entity.
   * Use the latest feature store snapshot if available
   */
  def chordScoobiJob(repository: HdfsRepository, dictionary: Dictionary, store: FeatureStore, chordKey: Key,
                     latestDate: Date, snapshot: Option[FeatureStoreSnapshot],
                     outputPath: Path, codec: Option[CompressionCodec]): ScoobiAction[Unit] = ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>

    lazy val entities = getEntities(repository, chordKey)

    Chord.readFacts(repository, store, latestDate, snapshot).map { facts =>

      /** get only the facts for the chord entities */
      val entitiesFacts: DList[PrioritizedFact] = filterFacts(facts, entities)
      /**
       * 1. group by entity and feature id
       * 2. for a given entity and feature id, get the latest facts, with the lowest priority
       */
      val latestFacts: DList[PrioritizedFact] = getBestFacts(entitiesFacts, entities)

      val validated: DList[PrioritizedFact] = validateFacts(latestFacts, dictionary, store, snapshot)

      validated.valueToSequenceFile(outputPath.toString, overwrite = true).persistWithCodec(codec); ()
    }
  }.flatten

  /**
   * filter out the facts which are not in the entityMap or
   * which date are greater than the required dates for this entity
   */
  def filterFacts(facts: DList[(Priority, SnapshotId \/ FactsetId, Fact)], getEntities: =>Entities): DList[PrioritizedFact] =
    facts.parallelDo(new DoFn[(Priority, SnapshotId \/ FactsetId, Fact), PrioritizedFact] {
      var entities: Entities = null
      override def setup() { entities = getEntities }
      override def cleanup(emitter: Emitter[PrioritizedFact]) {}

      override def process(input: (Priority, SnapshotId \/ FactsetId, Fact), emitter: Emitter[PrioritizedFact]) {
        val (fact, priority) = (input._3, input._1)
        if (entities.keep(fact)) emitter.emit((priority, fact))
      }
    })

  /**
   * Get the list of facts with the best priority and the most recent for each entity (and required entity date)
   */
  def getBestFacts(facts: DList[PrioritizedFact], getEntities: =>Entities): DList[PrioritizedFact] =
    facts
      .groupBy { case (p, f) => (f.entity, f.featureId.toString) }
      .parallelDo(new DoFn[((String, String), Iterable[PrioritizedFact]), PrioritizedFact] {
      var entities: Entities = null
      override def setup() { entities = getEntities }
      override def process(input: ((String, String), Iterable[PrioritizedFact]), emitter: Emitter[PrioritizedFact]) {
        input match {
          case ((entityId, featureId), fs) =>
            entities.keepBestFacts(entityId, fs).collect { case (date, priority, Some(fact)) if !fact.isTombstone =>
              emitter.emit((priority, fact.withEntity(fact.entity + ":" + Date.unsafeFromInt(date).hyphenated)))
            }
        }; ()
      }
      override def cleanup(emitter: Emitter[PrioritizedFact]) { }
    })

  /**
   * Validate that facts are in the dictionary with the right encoding
   */
  def validateFacts(facts: DList[PrioritizedFact], dictionary: Dictionary, store: FeatureStore, incremental: Option[FeatureStoreSnapshot]): DList[PrioritizedFact] = {
    // for each priority we get its snapshot id or factset id
    val priorities: util.Map[Priority, String] =
      mapAsJavaMap((incremental     .map(i =>  (Priority.Max, s"Snapshot '${i.snapshotId.render}'")) ++
        store.factsetIds.map(fs => (fs.priority,  s"Factset  '${fs.value.render}'"))).toMap.withDefault(p => s"Unknown, priority $p"))

    facts.map { case (priority, fact) =>
      Value.validateFact(fact, dictionary).disjunction match {
        case -\/(e) => Crash.error(Crash.DataIntegrity, s"A critical error has occurred, a value in ivory no longer matches the dictionary: $e ${priorities.get(priority)}")
        case \/-(v) => (priority, v)
      }
    }
  }

  /**
   * Read facts from a FeatureStore, up to a given date
   * If a FeatureStore snapshot is given we use it to retrieve the latest values
   */
  def readFacts(repository: HdfsRepository, featureStore: FeatureStore,
                latestDate: Date, featureStoreSnapshot: Option[FeatureStoreSnapshot]): ScoobiAction[DList[(Priority, SnapshotId \/ FactsetId, Fact)]] = {
    featureStoreSnapshot match {
      case None =>
        factsFromIvoryStoreTo(repository, featureStore, latestDate).failError("cannot read facts")
          .map(_.map { case (p, fid, f) => (p, fid.right[SnapshotId], f) })

      case Some(snapshot) =>
        val path          = repository.toIvoryLocation(Repository.snapshot(snapshot.snapshotId)).toHdfsPath
        val newFactsets   = featureStore diff snapshot.store

        for {
          oldFacts      <- factsFromIvoryStoreBetween(repository, snapshot.store, snapshot.date, latestDate).exitOnParseError
          _             <- ScoobiAction.log(s"Reading factsets up to '$latestDate'\n${newFactsets.factsets}")
          newFacts      <- factsFromIvoryStoreTo(repository, newFactsets, latestDate).exitOnParseError
          factsetFacts  =  (oldFacts ++ newFacts).map { case (p, fid, f) => (p, fid.right[SnapshotId], f) }
          snapshotFacts <- factsFromPath(path).exitOnParseError.map(_.map((Priority.Max, snapshot.snapshotId.left[FactsetId], _)))
        } yield factsetFacts ++ snapshotFacts
    }
  }

  private def factsFromPath(path: Path): ScoobiAction[DList[ParseError \/ Fact]] =
    ScoobiAction.scoobiConfiguration.map(sc => FlatFactThriftLoader(path.toString).loadScoobi(sc))

  implicit class ScoobiParsedListExitOps[A : WireFormat](action: ScoobiAction[DList[ParseError \/ A]]) {
    def exitOnParseError: ScoobiAction[DList[A]] = action.failError("cannot read facts")
  }

  implicit class ScoobiParsedListFailOps[E, A : WireFormat](action: ScoobiAction[DList[E \/ A]]) {
    def failError(message: String): ScoobiAction[DList[A]] =
      action.map { list =>
        list.map {
          case -\/(e) => Crash.error(Crash.ResultTIO, message)
          case \/-(a) => a
        }
      }
  }

  def getEntities(repository: Repository, chordKey: Key): Entities =
    deserialiseEntities(repository, chordKey).run.unsafePerformIO match {
      case Ok(m)    => m
      case Error(e) => Crash.error(Crash.Serialization, "Can not deserialise chord map - " + Result.asString(e))
    }

}
