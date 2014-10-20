package com.ambiata.ivory.operation.validation

import com.ambiata.notion.core.HdfsLocation
import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, Value => _, _}, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.scoobi._, WireFormats._, FactFormats._
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.metadata.Metadata._

sealed trait Validate {
  val counterGroup = "VALIDATION"
  val parseErrorCounterName = "PARSE_ERRORS"
  val encodingErrorCounterName = "ENCODING_ERRORS"

  def countRecords[A: WireFormat](dlist: DList[A], group: String, name: String): DList[A] =
    dlist.parallelDo((input: A, counters: Counters) => {
      counters.incrementCounter(group, name, 1)
      input
    })

  def getCounter(name: String)(implicit sc: ScoobiConfiguration): Long =
    (for {
      grp     <- Option(sc.counters.getGroup(counterGroup))
      counter <- Option(grp.findCounter(name))
    } yield counter.getValue).getOrElse(0)

  def exec(output: Path): ScoobiAction[Long] = for {
    sc <- ScoobiAction.scoobiConfiguration
    j  <- scoobiJob
    _  <- ScoobiAction.safe(j.toTextFile(output.toString, overwrite = true).persist(sc))
  } yield getCounter(parseErrorCounterName)(sc) + getCounter(encodingErrorCounterName)(sc)

  def scoobiJob: ScoobiAction[DList[String]]
}

case class ValidateStoreHdfs(repo: HdfsRepository, store: FeatureStore, dict: Dictionary, includeOverridden: Boolean) extends Validate {
  def scoobiJob: ScoobiAction[DList[String]] =
    factsFromIvoryStore(repo, store).map(input => {
      val errors: DList[String] = countRecords(input.collect {
        case -\/(e) => e.message
      }, counterGroup, parseErrorCounterName)

      val facts: DList[(Priority, FactsetId, Fact)] = input.collect {
        case \/-(s) => s
      }

      // remove duplicates, taking the fact with the highest priority
      val reduced: DList[(FactsetId, Fact)] =
        if(!includeOverridden && store.factsets.size > 1) {
          val byKey = facts.map({ case (p, fs, f) => (f.coordinateString('|'), (p, fs, f)) }).groupByKey
          val ord: Order[(Priority, FactsetId, Fact)] = Order.orderBy({ case (p, _, _) => p })
          byKey.reduceValues(Reduction.minimum(ord)).map({ case (_, (p, fs, f)) => (fs, f) })
        } else {
          facts.map({ case (_, fs, f) => (fs, f) })
        }

      val validated: DList[Validation[String, Fact]] =
        reduced.map({ case (fs, f) =>
          dict.byFeatureId.get(f.featureId).map(fm =>
            Value.validateFact(f, dict).leftMap(e => s"${e} - Fact set '${fs}'")
          ).getOrElse(s"Dictionary entry '${f.featureId}' doesn't exist!".failure)
        })

      val validationErrors: DList[String] = countRecords(validated.collect {
        case Failure(e) => e
      }, counterGroup, encodingErrorCounterName)

      errors ++ validationErrors
    })
}

case class ValidateFactSetHdfs(repo: HdfsRepository, factset: FactsetId, dict: Dictionary) extends Validate {

  def scoobiJob: ScoobiAction[DList[String]] =
    factsFromIvoryFactset(repo, factset).map(input => {
      val errors: DList[String] = countRecords(input.collect {
        case -\/(e) => e.message
      }, counterGroup, parseErrorCounterName)

      val facts: DList[Fact] = input.collect {
        case \/-(s) => s
      }

      val validated: DList[Validation[String, Fact]] = facts.map(f => Value.validateFact(f, dict))

      val validationErrors: DList[String] = countRecords(validated.collect {
        case Failure(e) => e
      }, counterGroup, encodingErrorCounterName)

      errors ++ validationErrors
    })
}

object Validate {

  def validateHdfsStore(repoPath: Path, store: FeatureStoreId, output: Path, includeOverridden: Boolean): ScoobiAction[Long] = for {
    r <- ScoobiAction.scoobiConfiguration.map(sc => HdfsRepository(HdfsLocation(repoPath.toString), IvoryConfiguration.fromScoobiConfiguration(sc)))
    d <- ScoobiAction.fromResultTIO(latestDictionaryFromIvory(r))
    s <- ScoobiAction.fromResultTIO(featureStoreFromIvory(r, store))
    c <- ValidateStoreHdfs(r, s, d, includeOverridden).exec(output)
  } yield c

  def validateHdfsFactSet(repoPath: Path, factset: FactsetId, output: Path): ScoobiAction[Long] = for {
    r <- ScoobiAction.scoobiConfiguration.map(sc => HdfsRepository(HdfsLocation(repoPath.toString), IvoryConfiguration.fromScoobiConfiguration(sc)))
    d <- ScoobiAction.fromResultTIO(latestDictionaryFromIvory(r))
    c <- ValidateFactSetHdfs(r, factset, d).exec(output)
  } yield c
}
