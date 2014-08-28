package com.ambiata.ivory.storage.legacy.fatrepo

import com.ambiata.ivory.storage.repository.Repositories

import scalaz.{DList => _, _}, effect._
import scala.math.{Ordering => SOrdering}
import org.apache.hadoop.fs.Path
import org.joda.time.{DateTimeZone, LocalDate}
import org.joda.time.format.DateTimeFormat
import org.apache.commons.logging.LogFactory

import com.ambiata.ivory.core._
import com.ambiata.ivory.data.StoreDataUtil
import com.ambiata.poacher.scoobi.ScoobiAction
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._, IvoryT.fromResultT

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

  type ImportFactsFunc = (FactsetId, ReferenceIO, DateTimeZone) => IvoryTIO[Unit]

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.Import")

  def onStore(importFacts: ImportFactsFunc, timezone: DateTimeZone): IvoryTIO[FactsetId] = {
    val start = System.currentTimeMillis
    for {
      _        <- createRepo
      t1 = {
        val x = System.currentTimeMillis
        println(s"created repository in ${x - start}ms")
        x
      }
      factset  <- Factsets.allocateId
      t3 = {
        val x = System.currentTimeMillis
        println(s"created fact set in ${x - t1}ms")
        x
      }
      repo     <- IvoryT.repository[ResultTIO]
      _        <- importFacts(factset, repo.toReference(repo.errors </> factset.render), timezone)
      t4 = {
        val x = System.currentTimeMillis
        println(s"imported fact set in ${x - t3}ms")
        x
      }
      _        <- Metadata.incrementFeatureStore(factset)
      // TODO: Update Commit
      t5 = {
        val x = System.currentTimeMillis
        println(s"created store in ${x - t4}ms")
        x
      }
    } yield factset
  }

  def createRepo: IvoryTIO[Unit] = IvoryT.fromResultT(repo => for {
    _  <- ResultT.ok[IO, Unit](logger.debug(s"Going to create repository '${repo.root.path}'"))
    e  <- repo.toStore.exists(repo.root)
    _  <- if(!e) {
      logger.debug(s"Path '${repo.root.path}' doesn't exist, creating")
      val res = Repositories.create(repo)
      logger.info(s"Repository '${repo.root.path}' created")
      res
    } else {
      logger.info(s"Repository already exists at '${repo.root.path}', not creating a new one")
      ResultT.ok[IO, Unit](())
    }
  } yield ())
}
