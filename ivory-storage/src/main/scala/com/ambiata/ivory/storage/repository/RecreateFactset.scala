package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.fact.{Factsets, Namespaces}
import com.ambiata.ivory.storage.control._
import com.ambiata.notion.core.KeyName
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.poacher.hdfs._
import org.joda.time.DateTime
import org.apache.hadoop.fs.Path
import MemoryConversions._

import scalaz._, Scalaz._

/**
 * This is used to recreate factsets so they have the latest format/compression/block size
 */
object RecreateFactset {

  def recreateFactsets(repository: Repository, factsets: List[FactsetId]): IvoryTIO[RecreatedFactsets] =
    factsets.foldLeftM[IvoryTIO, RecreatedFactsets](RecreatedFactsets(List[OriginalFactset](), factsets))((recreated, fid) =>
      recreateFactset(repository, fid).map(recreated.addCompleted).on(_.mapError(e => Result.prependThis(e, recreated.failString(fid)))))

  def recreateFactset(repository: Repository, factsetId: FactsetId): IvoryTIO[OriginalFactset] =
    IvoryT.read[RIO] >>= (read => IvoryT.fromRIO(for {
      hr         <- repository.asHdfsRepository
      config     <- Metadata.configuration.toIvoryT(repository).run(read)
      dictionary <- Metadata.latestDictionaryFromIvory(repository)
      factset    <- Factsets.factset(repository, factsetId)
      factsetPath = hr.toIvoryLocation(Repository.factset(factsetId)).toHdfsPath
      namespaces  = factset.partitions.groupBy(_.value.namespace).toList.map({
        case (namespace, partitions) =>
          namespace -> partitions.foldMap(_.bytes).toLong.bytes
      })
      tmpOut     <- Repository.tmpDir("recreate").map(hr.toIvoryLocation)
      _          <- RecreateFactsetJob.run(hr
                                           , dictionary
                                           , namespaces
                                           , factset
                                           , tmpOut.toHdfsPath
                                           , 1.gb // 1GB per reducer
                                           )
      expired     = hr.toIvoryLocation(Repository.tmp("expired", KeyName.unsafe(factset.id.render + "." + DateTime.now(config.timezone).toString("yyyyMMddhhmmss"))))
      _          <- commitFactset(factsetPath, expired.toHdfsPath, tmpOut.toHdfsPath).run(hr.configuration)
      _          <- Factsets.updateFactsetMetadata(hr, factset.id)
    } yield OriginalFactset(factsetId, expired)))

  def commitFactset(factset: Path, expired: Path, tmp: Path): Hdfs[Unit] = for {
    _ <- Hdfs.mkdir(expired.getParent)
    _ <- Hdfs.mv(factset, expired)
    _ <- Hdfs.mv(tmp, factset)
  } yield ()

}

case class OriginalFactset(factsetId: FactsetId, path: HdfsIvoryLocation) {
  def stringValue: String =
    s"${factsetId.render} -> ${path.show}"
}

case class RecreatedFactsets(completed: List[OriginalFactset], incompleted: List[FactsetId]) {

  def addCompleted(orig: OriginalFactset): RecreatedFactsets =
    RecreatedFactsets(orig :: completed, incompleted.filter(_ != orig.factsetId))

  def successString: String =
    s"""Successful recreations with their original data:
       |${completed.map(_.stringValue).mkString("\n")}""".stripMargin

  def failString(failedId: FactsetId): String =
    s"""Failed to recreate all factsets!
       |${successString}
       |
       |Failed recreation: ${failedId.render}
       |
       |Not attempted: ${incompleted.map(_.render).mkString(", ")}""".stripMargin
}
