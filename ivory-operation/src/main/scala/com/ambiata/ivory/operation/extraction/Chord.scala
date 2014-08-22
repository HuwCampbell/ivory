package com.ambiata.ivory.operation.extraction

import com.nicta.scoobi.Scoobi._
import org.apache.commons.logging.LogFactory
import scala.util.matching.Regex
import scalaz.{DList => _, Store => _, _}, Scalaz._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.poacher.scoobi._
import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.operation.validation._
import com.ambiata.poacher.hdfs._
import Entities._
import IvoryStorage._
import com.ambiata.ivory.scoobi._
import FlatFactThriftStorageV1._

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
   * Finally store the dictionary alongside the Chord
   */
  def createChord(repository: Repository, entitiesRef: ReferenceIO, outputRef: ReferenceIO, tmp: ReferenceIO, takeSnapshot: Boolean): ResultTIO[Unit] = for {
    _                   <- checkThat(repository, repository.isInstanceOf[HdfsRepository], "Chord only works on HDFS repositories at this stage.")
    _                   <- checkThat(outputRef, outputRef.store.isInstanceOf[HdfsStore], s"Currently output path must be on HDFS. Given value is $outputRef")
    entities            <- Entities.readEntitiesFrom(entitiesRef)
    _                   <- logInfo(s"Earliest date in chord file is '${entities.earliestDate}'")
    _                   <- logInfo(s"Latest date in chord file is '${entities.latestDate}'")
    store               <- Metadata.latestFeatureStoreOrFail(repository)
    snapshot            <- if (takeSnapshot) Snapshot.takeSnapshot(repository, entities.earliestDate, incremental = true).map(Option.apply)
                           else              SnapshotMeta.latest(repository, entities.earliestDate)
    _                   <- runChordOnHdfs(repository, store, entities, outputRef, tmp, snapshot)
    _                   <- storeDictionary(repository, outputRef)
  } yield ()

  /**
   * Run the chord extraction on Hdfs
   */
  def runChordOnHdfs(repository: Repository, store: FeatureStore, entities: Entities, outputRef: ReferenceIO, tmp: ReferenceIO, incremental: Option[SnapshotMeta]): ResultTIO[Unit] = {
    val chordRef = tmp </> FilePath(java.util.UUID.randomUUID.toString)
    for {
      hr                   <- downcast[Repository, HdfsRepository](repository, "Chord only works on HDFS repositories at this stage.")
      outputStore          <- downcast[Any, HdfsStore](outputRef.store, s"Currently output path must be on HDFS. Given value is $outputRef")
      outputPath           =  (outputStore.base </> outputRef.path).toHdfs
      _                    <- serialiseEntities(entities, chordRef)
      featureStoreSnapshot <- incremental.traverseU(meta => FeatureStoreSnapshot.fromSnapshotIdAfter(repository, meta.snapshotId, entities.earliestDate)).map(_.flatten)
      dictionary           <- dictionaryFromIvory(repository)
      _                    <- chordScoobiJob(hr, dictionary, store, chordRef, entities.latestDate, featureStoreSnapshot, outputPath, hr.codec).run(hr.scoobiConfiguration)
    } yield ()
  }

  /**
   * Persist facts which are the latest corresponding to a set of dates given for each entity.
   * Use the latest feature store snapshot if available
   */
  def chordScoobiJob(repository: Repository, dictionary: Dictionary, store: FeatureStore, chordReference: ReferenceIO,
                     latestDate: Date, snapshot: Option[FeatureStoreSnapshot],
                     outputPath: Path, codec: Option[CompressionCodec]): ScoobiAction[Unit] = ScoobiAction.scoobiJob { implicit sc: ScoobiConfiguration =>

      lazy val entities = getEntities(chordReference)

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
        input match { case ((entityId, featureId), fs) =>
          entities.keepBestFact(entityId, fs).collect { case (date, priority, Some(fact)) if !fact.isTombstone =>
            emitter.emit((priority, fact.withEntity(fact.entity + ":" + Date.unsafeFromInt(date).hyphenated)))
          }
        }
      }
      override def cleanup(emitter: Emitter[PrioritizedFact]) { }
    })

  /**
   * Validate that facts are in the dictionary with the right encoding
   */
  def validateFacts(facts: DList[PrioritizedFact], dictionary: Dictionary, store: FeatureStore, incremental: Option[FeatureStoreSnapshot]): DList[PrioritizedFact] = {
    // for each priority we get its snapshot id or factset id
    lazy val priorities: Map[Priority, String] =
      (incremental     .map(i =>  (Priority.Max, s"Snapshot '${i.snapshotId.render}'")).toList ++
       store.factsetIds.map(fs => (fs.priority,  s"Factset  '${fs.value.render}'"))).toMap.withDefault(p => s"Unknown, priority $p")

    facts.map { case (priority, fact) =>
      Validate.validateFact(fact, dictionary).disjunction match {
        case -\/(e) => sys.error(s"A critical error has occurred, a value in ivory no longer matches the dictionary: $e ${priorities.get(priority)}")
        case \/-(v) => (priority, v)
      }
    }
  }

  /**
   * Read facts from a FeatureStore, up to a given date
   * If a FeatureStore snapshot is given we use it to retrieve the latest values
   */
  def readFacts(repository: Repository, featureStore: FeatureStore,
                latestDate: Date, featureStoreSnapshot: Option[FeatureStoreSnapshot]): ScoobiAction[DList[(Priority, SnapshotId \/ FactsetId, Fact)]] = {
    featureStoreSnapshot match {
      case None =>
        factsFromIvoryStoreTo(repository, featureStore, latestDate).failError("cannot read facts")
          .map(_.map { case (p, fid, f) => (p, fid.right[SnapshotId], f) })

      case Some(snapshot) =>
        val path          =  repository.snapshot(snapshot.snapshotId).toHdfs
        val newFactsets   =  featureStore diff snapshot.store

        for {
          oldFacts      <- factsFromIvoryStoreBetween(repository, snapshot.store, snapshot.date, latestDate).exitOnParseError
          _             <- ScoobiAction.log(s"Reading factsets '${newFactsets.factsets}' up to '$latestDate'")
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
          case -\/(e) => sys.error(message)
          case \/-(a) => a
        }
      }
  }

  def storeDictionary(repository: Repository, outputRef: ReferenceIO): ResultTIO[Unit] =
    dictionaryFromIvory(repository).flatMap { dictionary =>
      DictionaryTextStorageV2.toStore(outputRef </> ".dictionary", dictionary)
    }

  def getEntities(chordReference: ReferenceIO): Entities =
    deserialiseEntities(chordReference).run.unsafePerformIO match {
      case Ok(m)    => m
      case Error(e) => Crash.error(Crash.Serialization, "Can not deserialise chord map - " + Result.asString(e))
    }

}
