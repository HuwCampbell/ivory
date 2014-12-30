package com.ambiata.ivory.storage.partition

import com.ambiata.mundane.control._
import com.ambiata.notion.core._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.manifest._
import scalaz._, Scalaz._, \&/._

object Partitions {
  def getFromFactset(repository: Repository, factset: FactsetId): RIO[List[Sized[Partition]]] =
    FactsetManifest.io(repository, factset).read.flatMap({
      case None =>
        scrapeFromFactset(repository, factset)
      case Some(manifest) =>
        manifest.partitions.pure[RIO]
    })

  def scrapeFromFactset(repository: Repository, factset: FactsetId): RIO[List[Sized[Partition]]] =
    for {
      keys       <- repository.store.list(Repository.factset(factset)).map(_.map(_.dropRight(1).drop(2)).filter(_ != Key.Root).distinct)
      partitions <- keys.traverseU(key => for {
          p <- RIO.fromDisjunction[Partition](Partition.parseNamespaceDateKey(key).disjunction.leftMap(This.apply))
          s <- IvoryLocation.size(repository.toIvoryLocation(key))
        } yield Sized(p, s))
    } yield partitions.sorted

  /** As Russell would say, this is fraught with danger, it is to be used very carefully and
      should be limited to expanding for use in MultiInputFormats which can't handle dealing
      with too many paths. The basic premise is that it produces a series of globs to minimize
      the amount of MultipleInputs required to start a job over the factsets.  */
  def globs(hdfs: HdfsRepository, factset: FactsetId, partitions: List[Partition]): List[String] =
    partitions.grouped(100).toList.map(ps =>
      hdfs.toIvoryLocation(Repository.factset(factset)).toHdfsPath + "/{" + ps.map(p => p.key.name).mkString(",") + "}")
}
