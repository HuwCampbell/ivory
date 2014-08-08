package com.ambiata.ivory.storage.legacy.fatrepo

import scalaz.{DList => _, _}, effect._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTimeZone, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.StoreDataUtil
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._

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
      factset  <- Factsets.allocateId(repo)
      t3 = {
        val x = System.currentTimeMillis
        println(s"created fact set in ${x - t1}ms")
        x
      }
      _        <- importFacts(repo, factset, repo.toReference(repo.errors </> factset.render), timezone)
      t4 = {
        val x = System.currentTimeMillis
        println(s"imported fact set in ${x - t3}ms")
        x
      }
      _        <- CreateFeatureStore.increment(repo, factset)
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
}
