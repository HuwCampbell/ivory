package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.BytesQuantity
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.joda.time.DateTimeZone

import scalaz.effect._

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
object Ingest {

  private implicit val logger = LogFactory.getLog("ivory.repository.fatrepo.Import")

  /**
   * Ingest facts in a newly created repository if necessary.
   *
   * This creates a new factset and a new feature store
   *
   * @param input input is the input directory containg facts to ingest
   * @param namespace name is an optional Namespace Name if the directory only contains facts for a single namespace
   * @param timezone each fact has a date and time but we must specify the timezone when importing
   * @param optimal size of each reducer ingesting facts
   * @param format text or thrift
   */
  def ingestFacts(repository: Repository, input: ReferenceIO, namespace: Option[Name],
                  timezone: DateTimeZone, optimal: BytesQuantity, format: Format): ResultTIO[FactsetId] =
    for {
      _         <- Repositories.create(repository).timed("created repository")
      factsetId <- Factsets.allocateFactsetId(repository).timed("created fact set")
      _         <- importFactset(repository, factsetId, input, namespace, timezone, optimal, format)
      _         <- Metadata.incrementStore(repository, factsetId).timed("created store")
    } yield factsetId

  def importFactset(repository: Repository, factsetId: FactsetId,
                    input: ReferenceIO, namespace: Option[Name], timezone: DateTimeZone, optimal: BytesQuantity, format: Format): ResultTIO[Unit] = {
    val errors = repository.toReference(repository.errors </> factsetId.render)
    for {
      hr              <- downcast[Repository, HdfsRepository](repository, "Currently only support HDFS repository")
      dictionary      <- dictionaryFromIvory(repository)
      path            <- Reference.hdfsPath(input)
      namespacesSizes <- Namespaces.namespaceSizes(path, namespace).run(conf)
      _               <- EavtTextImporter.onStore(repository, dictionary, factsetId, namespace, input, errors, timezone, namespacesSizes, optimal, format)
    } yield ()
  }

}
