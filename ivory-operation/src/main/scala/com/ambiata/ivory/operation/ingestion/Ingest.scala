package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.control._
import com.ambiata.ivory.storage.fact._
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
   * @param inputs inputs is the input directories containing facts to ingest
   * @param timezone each fact has a date and time but we must specify the timezone when importing
   * @param optimal size of each reducer ingesting facts
   */
  def ingestFacts(repository: Repository, cluster: Cluster, inputs: List[(FileFormat, Option[Name], IvoryLocation)],
                  timezone: Option[DateTimeZone], optimal: BytesQuantity): IvoryTIO[FactsetId] =
    for {
      factsetId <- Factsets.allocateFactsetIdI(repository)
      _         <- FactImporter.importFacts(repository, cluster, optimal, factsetId, inputs, timezone)
      _         <- Factsets.updateFeatureStore(factsetId).toIvoryT(repository)
    } yield factsetId
}
