package com.ambiata.ivory.operation.statistics

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.fact.Factsets

import com.ambiata.notion.core._
import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.mundane.control._

sealed trait FactStatsEntry
case class NumericalFactStatsEntry() extends FactStatsEntry
case class CategoricalFactStatsEntry() extends FactStatsEntry

case class FactsStats(stats: List[FactStatsEntry])

object FactStats {

  /** Create numerical and categorical stats for a given factset.
   *  Store them as a json blob in `_stats` file under the factset dir */
  def factset(repo: Repository, factsetId: FactsetId): RIO[FactStats] = for {
    hdfs       <- repo.asHdfsRepository
    factset    <- Factsets.factset(repo, factsetId)
    output      = hdfs.toIvoryLocation((Repository.factset(factsetId) / "_stats"))
    exists     <- Hdfs.exists(output.toHdfsPath).run(hdfs.configuration)
    _          <- RIO.when(exists, RIO.fail(s"Factset ${factsetId.render} already has stats!"))
    dictionary <- Metadata.latestDictionaryFromIvory(repo)
    datasets    = Datasets.empty.add(Priority.Min, Dataset.factset(factset))
    tmpLoc     <- Repository.tmpLocation(hdfs, "stats").flatMap(_.asHdfsIvoryLocation)
    _          <- FactStatsJob.run(hdfs, dictionary, datasets, tmpLoc)
    lines      <- Hdfs.globLines(tmpLoc.toHdfsPath, "*").run(hdfs.configuration).map(_.toList)
    stats      <- fromJsonLines(lines)
    _          <- repo.store.utf8.write(Repository.factset(factsetId) / "_stats", stats.asJson.nospaces)
  } yield ()

  def fromJsonLines(lines: List[String]): RIO[FactStats] = {
    RIO.ok("")
  }
}
