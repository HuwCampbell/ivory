package com.ambiata.ivory
package storage
package repository

import com.ambiata.mundane.store.Key
import com.ambiata.poacher.hdfs._
import com.ambiata.poacher.scoobi._
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.ivory.data._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.poacher.scoobi.ScoobiAction.scoobiJob
import com.ambiata.ivory.storage.fact.{Namespaces, Versions}
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.repository.RecreateData._
import com.ambiata.mundane.control.{ResultT, ResultTIO}
import com.ambiata.mundane.io._
import com.nicta.scoobi.Scoobi._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import IvoryStorage._

import scalaz.Scalaz._
import scalaz.effect.IO
import scalaz.Kleisli._
import scalaz.{DList => _, _}
import Namespaces._
import Stats._

/**
 * Recreate actions for recreating parts or all of a repository
 */
object Recreate { outer =>

  type RecreateAction[A] = ReaderT[ResultTIO, RecreateConfig, A]

  def all: RecreateAction[Unit] =
    log("====== Recreating metadata")  >> metadata >>
    log("====== Recreating factsets")  >> factsets >>
    log("====== Recreating snapshots") >> snapshots

  def metadata: RecreateAction[Unit] =
    log("****** Recreating dictionaries") >> dictionaries >>
    log("****** Recreating feature stores") >> featureStores

  def dictionaries: RecreateAction[Unit] =
    recreate(DICTIONARY, Repository.dictionaries) { conf =>
      fromHdfs(copyDictionaries(conf.hdfsFrom, conf.hdfsTo, conf.dryFor(RecreateData.DICTIONARY), conf.maxNumber))
    }

  def featureStores: RecreateAction[Unit] =
    recreate(STORE, Repository.featureStores) { conf =>
      fromHdfs(copyFeatureStores(conf.hdfsFrom, conf.hdfsTo, conf.clean, conf.dryFor(RecreateData.STORE), conf.maxNumber))
    }

  def factsets: RecreateAction[Unit] =
    recreate(FACTSET, Repository.factsets) { conf =>
      fromScoobi(copyFactsets(conf.hdfsFrom, conf.hdfsTo, conf.codec, conf.reducerSize, conf.dryFor(RecreateData.FACTSET), conf.maxNumber))
    }

  def snapshots: RecreateAction[Unit] =
    recreate(SNAPSHOT, Repository.snapshots) { conf =>
      fromScoobi(copySnapshots(conf.hdfsFrom, conf.hdfsTo, conf.codec, conf.dryFor(RecreateData.SNAPSHOT), conf.maxNumber))
    }

  /**
   * recreate a given set of data and log before/after count and size
   */
  private def recreate[A, V](data: RecreateData, key: Key)(action: RecreateConfig => RecreateAction[A]): RecreateAction[Unit] =
    configuration.flatMap { conf =>
      val name = data.plural
      val todo =
        log("Dry run!").when(conf.dryFor(data)) >>
        logStat("Number of "+name, conf.from, Stats.numberOf(key)) >>
        logStat("Size of "+name,   conf.from, Stats.showSizeOfInBytes(key)) >>
        action(conf) >>
        logStat("Number of "+name, conf.to, Stats.numberOf(key)) >>
        logStat("Size of "+name,   conf.to, Stats.showSizeOfInBytes(key))

      todo.unless(conf.recreateData.nonEmpty && conf.dryFor(data))
    }

  /**
   * DICTIONARIES
   */
  private def copyDictionaries(from: HdfsRepository, to: HdfsRepository, dry: Boolean, maxNumber: Option[Int]): Hdfs[Unit] = for {
    _        <- Hdfs.mkdir(to.toFilePath(Repository.dictionaries).toHdfs).unless(dry)
    paths    <- Hdfs.globPaths(from.toFilePath(Repository.dictionaries).toHdfs)
    existing <- Hdfs.globPaths(to.toFilePath(Repository.dictionaries).toHdfs).map(_.map(_.getName))
    missing  =  paths.filterNot(p => existing.contains(p.getName))
    _        <- missing.take(maxNumber.fold(missing.size)(identity)).traverse(copyDictionary(from, to, dry))
  } yield ()

  private def copyDictionary(from: HdfsRepository, to: HdfsRepository, dry: Boolean) = (path: Path) => for {
      dictId <- Hdfs.fromOption(Identifier.parse(path.getName), s"Could not parse '${path.getName}'")
      _      <- Hdfs.log(s"Copy dictionary ${path.getName} from ${from.toFilePath(Repository.dictionaryById(DictionaryId(dictId)))} to ${to.toFilePath(Repository.dictionaryById(DictionaryId(dictId)))}")
      dict   <- Hdfs.fromResultTIO(latestDictionaryFromIvory(from) >>= { dict: Dictionary =>
        dictionaryToIvory(to, dict)
      }).unless(dry)
    } yield ()

  /**
   * STORES
   */
  private def copyFeatureStores(from: HdfsRepository, to: HdfsRepository, clean: Boolean, dry: Boolean, maxNumber: Option[Int]): Hdfs[Unit] =
    Hdfs.mkdir(to.toFilePath(Repository.featureStores).toHdfs).unless(dry) >>
      (nonEmptyFactsetIds(from, to) tuple featureStoresPaths(from, to)).flatMap { case (factsets, featureStores) =>
        featureStores.take(maxNumber.fold(featureStores.size)(identity)).traverse(copyFeatureStore(from, to, clean, dry, factsets)).void
      }

  private def copyFeatureStore(from: HdfsRepository, to: HdfsRepository, clean: Boolean, dry: Boolean, filtered: List[FactsetId]) = (path: Path) =>
    for {
      featureStoreId <- Hdfs.fromOption(FeatureStoreId.parse(path.getName), s"Could not parse '${path.getName}'")
      _       <- Hdfs.log(s"Copy feature store ${featureStoreId} from ${from.toFilePath(Repository.featureStoreById(featureStoreId))} to ${to.toFilePath(Repository.featureStoreById(featureStoreId))}")
      store   <- Hdfs.fromResultTIO(featureStoreFromIvory(from, featureStoreId))
      cleaned <- cleanupFeatureStore(featureStoreId, store, filtered, clean)
      _       <- Hdfs.fromResultTIO(featureStoreToIvory(to, cleaned)).unless(dry)
    } yield ()

  private def cleanupFeatureStore(id: FeatureStoreId, featureStore: FeatureStore, setsToKeep: List[FactsetId], clean: Boolean): Hdfs[FeatureStore] = {
    val cleaned = if (clean) featureStore.filter(setsToKeep.toSet) else featureStore
    val removed = featureStore.factsetIds.map(_.value.render).diff(cleaned.factsetIds.map(_.value.render))
    Hdfs.log(s"Removed factsets '${removed.mkString(",")}' from feature store '${id.render}' as they are empty.").unless(removed.isEmpty) >>
    Hdfs.safe(cleaned)
  }

  private def featureStoresPaths(from: Repository, to: Repository): Hdfs[List[Path]] = for {
    paths         <- Hdfs.globFiles(from.toFilePath(Repository.featureStores).toHdfs)
    existingNames <- Hdfs.globFiles(to.toFilePath(Repository.featureStores).toHdfs).map(_.map(_.getName))
  } yield paths.filterNot(p => existingNames.contains(p.getName))

  /**
   * FACTSETS
   */
  private def copyFactsets(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], reducerSize: BytesQuantity, dry: Boolean, maxNumber: Option[Int]): ScoobiAction[Unit] =
    for {
      ids        <- ScoobiAction.fromHdfs(nonEmptyFactsetIds(from, to))
      _          <- ScoobiAction.log("non empty factset ids "+ids.map(_.render).mkString(","))
      dictionary <- ScoobiAction.fromResultTIO(Metadata.latestDictionaryFromIvory(from))
      _          <- ids.toList.take(maxNumber.fold(ids.size)(identity)).traverse(copyFactset(dictionary, from, to, codec, reducerSize, dry)).unless(dry)
    } yield ()

  def copyFactset(dictionary: Dictionary,
                  from: HdfsRepository, to: HdfsRepository,
                  codec: Option[CompressionCodec], reducerSize: BytesQuantity, dry: Boolean): FactsetId => ScoobiAction[Unit] = (id: FactsetId) =>
    for {
      _             <- ScoobiAction.log(s"Copy factset '${id.render}' from ${from.toFilePath(Repository.factset(id))} to ${to.toFilePath(Repository.factset(id))}")
      configuration <- ScoobiAction.scoobiConfiguration
      namespaces    <- ScoobiAction.fromHdfs(namespaceSizes(from.toFilePath(Repository.factset(id)).toHdfs))
      partitions    <- ScoobiAction.fromHdfs(Hdfs.globFiles(from.toFilePath(Repository.factset(id)).toHdfs, "*/*/*/*/*").filterHidden)
      version       <- ScoobiAction.fromResultTIO(Versions.read(from, id))
      _             <- {
        ScoobiAction.safe(RecreateFactsetJob.run(configuration, version, dictionary, namespaces, partitions, to.toFilePath(Repository.factset(id)).toHdfs, reducerSize, codec)) >>
        ScoobiAction.fromResultTIO(writeFactsetVersion(to, List(id)))
      }.unless(dry)
    } yield ()

  /**
   * SNAPSHOTS
   *
   * create a Scoobi job to copy all the snapshots paths as one big DList
   */
  private def copySnapshots(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec], dry: Boolean, maxNumber: Option[Int]): ScoobiAction[Unit] = {
    for {
      paths    <- ScoobiAction.fromHdfs(Hdfs.globPaths(from.toFilePath(Repository.snapshots).toHdfs))
      existing <- ScoobiAction.fromHdfs(Hdfs.globPaths(to.toFilePath(Repository.snapshots).toHdfs)).map(_.map(_.getParent.getName))
      missing  = paths.filterNot(p => existing.contains(p.getParent.getName))
      dlists   <- missing.take(maxNumber.fold(missing.size)(identity)).traverse(copySnapshot(from, to, codec))
      _        <- scoobiJob(dlists.reduce(_++_).persist(_)).unless(dry)
    } yield ()
  }

  import com.ambiata.ivory.storage.legacy.FlatFactThriftStorageV1._
  private def copySnapshot(from: HdfsRepository, to: HdfsRepository, codec: Option[CompressionCodec]) = (path: Path) =>
    ScoobiAction.log(s"Copy snapshot ${path.getName} from $path to ${to.toFilePath(Repository.snapshots).path+"/"+path.getName}") >>
    loadFacts(path).flatMap(storeFacts(path, to, codec))

  private def loadFacts(path: Path): ScoobiAction[DList[Fact]] =
    scoobiJob(FlatFactThriftLoader(path.toString).loadScoobi(_)).map(_.map(throwAwayErrors("Could not load facts")))

  private def storeFacts(path: Path,to: HdfsRepository, codec: Option[CompressionCodec]) = (facts: DList[Fact]) =>
    scoobiJob(FlatFactThriftStorer(new Path(to.toFilePath(Repository.snapshots).toHdfs, path.getName).toString, codec).storeScoobi(facts)(_))

  private def nonEmptyFactsetIds(from: Repository, to: Repository): Hdfs[List[FactsetId]] = {
    def getChildren(paths: List[Path]): Hdfs[Set[String]] =
      paths.traverse(p => Hdfs.globFiles(p, "*/*/*/*/*").filterHidden.map(ps => (p, ps.isEmpty)) ||| Hdfs.value((p, true))).map(_.filterNot(_._2).map(_._1.getName).toSet)

    for {
      paths            <- Hdfs.globPaths(from.toFilePath(Repository.factsets).toHdfs)
      existing         <- Hdfs.globPaths(to.toFilePath(Repository.factsets).toHdfs)
      children         <- getChildren(paths)
      existingChildren <- getChildren(existing)
      ids              <- children.diff(existingChildren).toList.sorted.traverseU(c => Hdfs.fromOption(FactsetId.parse(c), s"Can not parse Factset id '${c}'"))
    } yield ids
  }

  private def throwAwayErrors[E, A](message: String) = (ea: E \/ A) => ea match {
    case -\/(e) => Crash.error(Crash.DataIntegrity, s"$message '$e'")
    case \/-(a) => a
  }

  /** Execute a stat action and log the result */
  private def logStat[A](name: String, repository: Repository, stat: StatAction[A]): RecreateAction[Unit] =
    fromStat(repository, stat).log(value => s"$name in ${repository}: $value")

  /**
   * RECREATE ACTION methods
   */

  /** create actions */
  private def fromStat[A](repo: Repository, action: StatAction[A]): RecreateAction[A] =
    fromScoobi(ScoobiAction.scoobiConfiguration.flatMap(sc => ScoobiAction.fromResultTIO(action.run(StatConfig(sc.configuration, repo)))))

  private implicit def createKleisli[A](f: RecreateConfig => ResultTIO[A]): RecreateAction[A] =
    kleisli[ResultTIO, RecreateConfig, A](f)

  private def configuration: RecreateAction[RecreateConfig] =
    (config: RecreateConfig) => ResultT.ok[IO, RecreateConfig](config)

  private def log(message: String): RecreateAction[Unit] =
    (config: RecreateConfig) => ResultT.fromIO(config.logger(message))

  private def fromScoobi[A](action: ScoobiAction[A]): RecreateAction[A] =
    (c: RecreateConfig) => action.run(c.sc)

  private def fromHdfs[A](action: Hdfs[A]): RecreateAction[A] =
    fromScoobi(ScoobiAction.fromHdfs(action))

  private def fromResultTIO[A](r: ResultTIO[A]): RecreateAction[A] =
    (c: RecreateConfig) => r

  /** additional syntax */
  implicit class actionSyntax[A](action: RecreateAction[A]) {
    def log(f: A => String): RecreateAction[Unit] =
      action.flatMap(a => outer.log(f(a)))

    def when(condition: Boolean): RecreateAction[Unit] =
      if (condition) action.void else fromResultTIO(ResultT.ok[IO, Unit](()))

    def unless(condition: Boolean): RecreateAction[Unit] =
      when(!condition)
  }

}
