package com.ambiata.ivory.storage.legacy.fatrepo

import com.nicta.scoobi.Scoobi._
import scalaz.{DList => _, _}, Scalaz._, effect._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTimeZone, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.data.StoreDataUtil
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.poacher.hdfs._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._

/**
 * This workflow is designed to import features into an fat ivory repository,
 * one which contains all facts over all of time.
 *
 * Steps:
 * 1. Create empty repository if one doesn't exist
 * 2. Create an empty fact set to import the data feeds into
 * 3. Import the feeds into the fact set
 * 4. Create a new feature store:
 *    - Find the latest feature store
 *    - Create a new feature store containing the newly created fact set, and all the fact sets from the latest feature store
 *    - Use the previous feature store + 1 as the name of the new feature store
 */
object ImportWorkflow {

  type ImportFactsFunc = (Repository, FactsetId, ReferenceIO, DateTimeZone) => ResultTIO[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.Import")

  def onStore(repo: Repository, importFacts: ImportFactsFunc, timezone: DateTimeZone): ResultTIO[FactsetId] = {
    val start = System.currentTimeMillis
    for {
      _        <- createRepo(repo)
      t1 = {
        val x = System.currentTimeMillis
        println(s"created repository in ${x - start}ms")
        x
      }
      factset  <- createFactSet(repo)
      t3 = {
        val x = System.currentTimeMillis
        println(s"created fact set in ${x - t1}ms")
        x
      }
      _        <- importFacts(repo, factset, repo.toReference(repo.errors </> factset.name), timezone)
      t4 = {
        val x = System.currentTimeMillis
        println(s"imported fact set in ${x - t3}ms")
        x
      }
      sname    <- createStore(repo, factset)
      t5 = {
        val x = System.currentTimeMillis
        println(s"created store in ${x - t4}ms")
        x
      }
    } yield factset
  }

  def createRepo(repo: Repository): ResultTIO[Unit] = for {
    _  <- ResultT.ok[IO, Unit](logger.debug(s"Going to create repository '${repo.root.path}'"))
    e  <- repo.toStore.exists(repo.root)
    _  <- if(!e) {
      logger.debug(s"Path '${repo.root.path}' doesn't exist, creating")
      val res = CreateRepository.onStore(repo)
      logger.info(s"Repository '${repo.root.path}' created")
      res
    } else {
      logger.info(s"Repository already exists at '${repo.root.path}', not creating a new one")
      ResultT.ok[IO, Unit](())
    }
  } yield ()

  def nextFactset(repo: Repository): ResultTIO[FactsetId] = for {
    factsetPaths <- StoreDataUtil.listDir(repo.toStore, Repository.factsets)
  } yield FactsetId(nextName(factsetPaths.map(_.basename.path)))

  // TODO change this to use IdentifierStorage
  // TODO handle locking
  def createFactSet(repo: Repository): ResultTIO[FactsetId] = for {
    factset <- nextFactset(repo)
    _       <- repo.toStore.bytes.write(Repository.factsets </> FilePath(factset.name) </> ".allocated", scodec.bits.ByteVector.empty)
  } yield factset

  def listStores(repo: Repository): ResultTIO[List[String]] =
    repo.toStore.list(Repository.stores).map(_.map(_.basename.path))

  def latestStore(repo: Repository): ResultTIO[Option[String]] =
    listStores(repo).map(latestName)

  // TODO handle locking
  def createStore(repo: Repository, factset: FactsetId): ResultTIO[String] = for {
    names  <- listStores(repo)
    latest  = latestName(names)
    name    = nextName(names)
    _       = logger.debug(s"Going to create feature store '${name}'" + latest.map(l => s" based off feature store '${l}'").getOrElse(""))
    _      <- CreateFeatureStore.inRepository(repo, name, List(factset), latest)
  } yield name

  // TODO Replace all the below with Identifier
  def latestName(names: List[String]): Option[String] =
    latestNameWith(names, identity)

  def latestNameWith(names: List[String], incr: Int => Int): Option[String] = {
    val offsets = names.map(_.parseInt).collect({ case Success(i) => i })
    if(offsets.isEmpty) None else Some(zeroPad(incr(offsets.max)))
  }

  def firstName: String =
    zeroPad(0)

  def nextName(names: List[String]): String =
    latestNameWith(names, _ + 1).getOrElse(firstName)

  def zeroPad(i: Int): String =
    "%05d".format(i)
}
