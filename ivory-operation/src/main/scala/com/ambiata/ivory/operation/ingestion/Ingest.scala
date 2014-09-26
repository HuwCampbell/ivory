package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control.IvoryTIO
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.metadata._
import IvoryStorage._
import com.ambiata.mundane.io.BytesQuantity
import org.joda.time.DateTimeZone

/**
 * Import facts in an Ivory repository from an input path.
 *
 * The input path is expected to either:
 *
 *  - point to a list of directories where each directory is a namespace and contains partitions
 *    (sub-directories which names are dates, and containing the fact files)
 *
 *    input/
 *      demographics/
 *         year=2014/month=03/day=25/part1.txt
 *         year=2014/month=03/day=26/part1.txt
 *      data_usage/
 *         year=2014/month=03/day=25/part1.txt
 *         year=2014/month=03/day=26/part1.txt
 *
 *  In this case the name of each namespace directory must correspond to a namespace in the dictionary
 *
 *  - be a single directory containing the partitioned files  (if namespace: Option[Name] is defined)
 *
 *   input/
 *      year=2014/month=03/day=25/part1.txt
 *      year=2014/month=03/day=26/part1.txt
 *
 *   in this case the namespace value is taken as the Namespace name (and must exist in the dictionary)
 *
 * It is possible to ingest facts with a different importer than the EAVTTextImporter by
 *
 *  1. calling createNewFactsetId
 *  2. importing facts using: the new factsetId, the input reference, the optional namespace and the expected timezone
 *  3. calling updateFeatureStore to update the feature store and save the factset version
 */
object Ingest {
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
  def ingestFacts(repository: Repository, input: IvoryLocation, namespace: Option[Name],
                  timezone: DateTimeZone, optimal: BytesQuantity, format: Format): IvoryTIO[FactsetId] =
    for {
      factsetId <- Factsets.allocateFactsetIdI(repository)
      _         <- FactImporter.importFacts(repository, namespace, optimal, format, factsetId, input, timezone)
      _         <- updateFeatureStore(repository, factsetId)
    } yield factsetId

  /**
   * update the repository after the import of facts in a factset:
   *  - increment the feature store
   *  - write the factset version
   */
  def updateFeatureStore(repository: Repository, factsetId: FactsetId): IvoryTIO[FeatureStoreId] = (for {
    fs <- Metadata.incrementFeatureStore(List(factsetId))
    _  <- writeFactsetVersionI(List(factsetId))
    _  <- Metadata.incrementCommitFeatureStoreT(fs)
  } yield fs).toIvoryT(repository)

}
