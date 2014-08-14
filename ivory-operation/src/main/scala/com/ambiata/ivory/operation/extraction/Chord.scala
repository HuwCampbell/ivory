package com.ambiata.ivory.operation.extraction

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import java.util.HashMap
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import com.ambiata.mundane.io._
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.poacher.scoobi._
import com.ambiata.poacher.hdfs._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.legacy.fatrepo.ExtractLatestWorkflow
import com.ambiata.ivory.storage.metadata._, Metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.operation.validation._
import com.ambiata.poacher.hdfs._

case class Chord(repo: Repository, store: FeatureStoreId, entities: HashMap[String, Array[Int]], output: ReferenceIO, tmp: ReferenceIO, incremental: Option[SnapshotId], codec: Option[CompressionCodec]) {

  type PackedDate = Int
  // mappings of each entity to an array of target dates, represented as Ints and sorted from more recent to least
  type Mappings   = HashMap[String, Array[PackedDate]]

  // lexical order for a pair (Fact, Priority) so that
  // p1 < p2 <==> f1.datetime > f2.datetime || f1.datetime == f2.datetime && priority1 < priority2
  implicit val ord: Order[(Fact, Priority)] = Order.orderBy { case (f, p) => (-f.datetime.long, p) }

  val ChordName: String = "ivory-incremental-chord"

  def run: ResultTIO[Unit] = for {
    hr <- repo match {
      case h: HdfsRepository => ResultT.ok[IO, HdfsRepository](h)
      case _                 => ResultT.fail[IO, HdfsRepository]("Chord only works on HDFS repositories at this stage.")
    }
    // TODO we need to support having output on different store implementations (store scoobi output on hdfs then sync to another store)
    o  <- output match {
      case Reference(HdfsStore(_, root), p) => ResultT.ok[IO, Path]((root </> p).toHdfs)
      case _                                => ResultT.fail[IO, Path](s"Currently output path must be on HDFS. Given value is ${output}")
    }
    d  <- dictionaryFromIvory(repo)
    s  <- storeFromIvory(repo, store)
    (earliest, latest) = DateMap.bounds(entities)
    chordRef = tmp </> FilePath(java.util.UUID.randomUUID().toString)
    _  <- Chord.serialiseChords(chordRef, entities)
    in <- incremental.traverseU(snapId => for {
        sm <- SnapshotMeta.fromIdentifier(repo, snapId)
      _   = println(s"Snapshot store was '${sm.store}'")
      _   = println(s"Snapshot date was '${sm.date.string("-")}'")
      s  <- storeFromIvory(repo, sm.store)
    } yield (snapId, s, sm))
    _  <- hr.run.runScoobi(scoobiJob(hr, d, s, chordRef, latest, validateIncr(earliest, in), o, codec))
    _  <- DictionaryTextStorageV2.toStore(output </> FilePath(".dictionary"), d)
  } yield ()

  def validateIncr(earliest: Date, in: Option[(SnapshotId, FeatureStore, SnapshotMeta)]): Option[(SnapshotId, FeatureStore, SnapshotMeta)] =
    in.flatMap({ case i @ (_, _, sm) =>
      if(earliest isBefore sm.date) {
        println(s"Earliest date '${earliest}' in chord file is before snapshot date '${sm.date}' so going to skip incremental and pass over all data.")
        None
      } else {
        Some(i)
      }
    })

  /**
   * Persist facts which are the latest corresponding to a set of dates given for each entity
   */
  def scoobiJob(repo: Repository, dict: Dictionary, store: FeatureStore, chordReference: ReferenceIO, latestDate: Date, incremental: Option[(SnapshotId, FeatureStore, SnapshotMeta)], outputPath: Path, codec: Option[CompressionCodec]): ScoobiAction[Unit] =
    ScoobiAction.scoobiJob({ implicit sc: ScoobiConfiguration =>
      lazy val factsetMap: Map[Priority, SnapshotId \/ FactsetId] =
        (incremental.map(i => (Priority.Max, i._1.left)).toList ++ store.factsetIds.map(fs => (fs.priority, fs.value.right))).toMap

      Chord.readFacts(repo, store, latestDate, incremental).map { input =>

        // filter out the facts which are not in the entityMap or
        // which date are greater than the required dates for this entity
        val facts: DList[(Priority, Fact)] = input.map({
          case -\/(e) => sys.error("A critical error has occured, where we could not determine priority and namespace from partitioning: " + e)
          case \/-(v) => v
        }).parallelDo(new DoFn[(Priority, SnapshotId \/ FactsetId, Fact), (Priority, Fact)] {
          var mappings: Mappings = null
          override def setup() {
            mappings = Chord.getMappings(chordReference)
          }
          override def process(input: (Priority, SnapshotId \/ FactsetId, Fact), emitter: Emitter[(Priority, Fact)]) {
            input match { case (p, _, f) =>
              if(DateMap.keep(mappings, f.entity, f.date.year, f.date.month, f.date.day)) emitter.emit((p, f))
            }
          }
          override def cleanup(emitter: Emitter[(Priority, Fact)]) { }
        })

        /**
         * 1. group by entity and feature id
         * 2. for a given entity and feature id, get the latest facts, with the lowest priority
         */
        val latest: DList[(Priority, Fact)] =
          facts
            .groupBy { case (p, f) => (f.entity, f.featureId.toString) }
            .parallelDo(new DoFn[((String, String), Iterable[(Priority, Fact)]), (Priority, Fact)] {
              var mappings: Mappings = null
              override def setup() {
                mappings = Chord.getMappings(chordReference)
              }
              override def process(input: ((String, String), Iterable[(Priority, Fact)]), emitter: Emitter[(Priority, Fact)]) {
                input match { case ((entityId, featureId), fs) =>
                  // the required dates
                  val dates = mappings.get(entityId)

                  // we traverse all facts and for each required date
                  // we keep the "best" fact which date is just before that date
                  fs.foldLeft(dates.map((_, Priority.Min, None)): Array[(Int, Priority, Option[Fact])]) { case (ds, (priority, fact)) =>
                    val factDate = fact.date.int
                    ds.map {
                      case previous @ (date, p, None)    =>
                        // we found a first suitable fact for that date
                        if (factDate <= date) (date, priority, Some(fact))
                        else                  previous

                      case previous @ (date, p, Some(f)) =>
                        // we found a fact with a better time, or better priority if there is a tie
                        if (factDate <= date && (fact, priority) < ((f, p))) (date, priority, Some(fact))
                        else                                                 previous
                    }
                  }.collect({ case (d, p, Some(f)) => (p, f.withEntity(f.entity + ":" + Date.unsafeFromInt(d).hyphenated)) })
                   .foreach({ case (p, f) => if(!f.isTombstone) emitter.emit((p, f)) })
                }
              }
              override def cleanup(emitter: Emitter[(Priority, Fact)]) { }
            })

        val validated: DList[Fact] = latest.map({ case (p, f) =>
          Validate.validateFact(f, dict).disjunction.leftMap(e => e + " - " + factsetMap.get(p).map({
            case -\/(snapId)    => s"Snapshot '${snapId.render}'"
            case \/-(factsetId) => s"Factset '${factsetId.render}'"
          }).getOrElse("Unknown, priority " + p))
        }).map({
          case -\/(e) => sys.error("A critical error has occurred, a value in ivory no longer matches the dictionary: " + e)
          case \/-(v) => v
        })

        val toPersist = validated.valueToSequenceFile(outputPath.toString, overwrite = true)
        persist(codec.map(toPersist.compressWith(_)).getOrElse(toPersist))

        ()
      }
    }).flatten
}

object Chord {
  val ChordName: String = "ivory-incremental-chord"

  def onStore(repo: Repository, entities: ReferenceIO, output: ReferenceIO, tmp: ReferenceIO, takeSnapshot: Boolean, codec: Option[CompressionCodec]): ResultTIO[Unit] = for {
    es                  <- Chord.readChords(entities)
    (earliest, latest)   = DateMap.bounds(es)
    _                    = println(s"Earliest date in chord file is '${earliest}'")
    _                    = println(s"Latest date in chord file is '${latest}'")
    snap                <- if(takeSnapshot)
                             Snapshot.takeSnapshot(repo, earliest, true, codec).map({ case (s, p) => (s, Some(p)) })
                           else
                             latestSnapshot(repo, earliest)
    (store, id)         = snap
    _                   <- Chord(repo, store, es, output, tmp, id, codec).run
  } yield ()

  def readFacts(repo: Repository, store: FeatureStore, latestDate: Date, incremental: Option[(SnapshotId, FeatureStore, SnapshotMeta)]): ScoobiAction[DList[ParseError \/ (Priority, SnapshotId \/ FactsetId, Fact)]] = {
    import IvoryStorage._
    incremental match {
      case None =>
        factsFromIvoryStoreTo(repo, store, latestDate).map(_.map(_.map({ case (p, fid, f) => (p, fid.right[SnapshotId], f) })))
      case Some((snapId, s, sm)) => for {
        c <- ScoobiAction.scoobiConfiguration
        p = repo.snapshot(snapId).toHdfs
        o <- factsFromIvoryStoreBetween(repo, s, sm.date, latestDate) // read facts from already processed store from the last snapshot date to the latest date
        sd = store diff s
        _  = println(s"Reading factsets '${sd.factsets}' up to '${latestDate}'")
        n <- factsFromIvoryStoreTo(repo, sd, latestDate) // read factsets which haven't been seen up until the 'latest' date
        factsetData = (o ++ n).map(_.map({ case (p, fid, f) => (p, fid.right[SnapshotId], f) }))
      } yield factsetData ++ FlatFactThriftStorageV1.FlatFactThriftLoader(p.toString).loadScoobi(c).map(_.map((Priority.Max, snapId.left[FactsetId], _)))
    }
  }

  def latestSnapshot(repo: Repository, date: Date): ResultTIO[(FeatureStoreId, Option[SnapshotId])] = for {
    store  <- ExtractLatestWorkflow.latestStore(repo)
    latest <- SnapshotMeta.latest(repo, date)
  } yield (store, latest.map(_._1))

  def serialiseChords(ref: ReferenceIO, map: HashMap[String, Array[Int]]): ResultTIO[Unit] = {
    import java.io.ObjectOutputStream
    ref.run(store => path => store.unsafe.withOutputStream(path)(os => ResultT.safe({
      val bOut = new ObjectOutputStream(os)
      bOut.writeObject(map)
      bOut.close()
    })))
  }

  // TODO Change to thrift serialization, see #131
  def deserialiseChords(ref: ReferenceIO): ResultTIO[HashMap[String, Array[Int]]] = {
    import java.io.{ByteArrayInputStream, ObjectInputStream}
    ref.run(store => path => store.bytes.read(path).flatMap(bytes =>
      ResultT.safe((new ObjectInputStream(new ByteArrayInputStream(bytes.toArray))).readObject.asInstanceOf[HashMap[String, Array[Int]]])))
  }

  def readChords(ref: ReferenceIO): ResultTIO[HashMap[String, Array[Int]]] =
    ref.run(s => s.utf8.read).map(DateMap.chords)

  def getMappings(chordReference: ReferenceIO)(implicit sc: ScoobiConfiguration): HashMap[String, Array[Int]] =
    deserialiseChords(chordReference).run.unsafePerformIO() match {
      case Ok(m)    => m
      case Error(e) => sys.error("Can not deserialise chord map - " + Result.asString(e))
    }
}
