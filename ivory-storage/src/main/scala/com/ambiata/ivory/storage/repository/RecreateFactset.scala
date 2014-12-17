package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.legacy.IvoryStorage
import com.ambiata.ivory.storage.fact.{Namespaces, Versions}
import com.ambiata.ivory.storage.control._

import com.ambiata.notion.core.KeyName
import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.poacher.hdfs._
import org.joda.time.DateTime
import org.apache.hadoop.fs.Path
import MemoryConversions._

import scalaz._, Scalaz._, effect._

/**
 * This is used to recreate factsets so they have the latest format/compression/block size
 */
object RecreateFactset {

  def recreateFactsets(repository: Repository, factsets: List[FactsetId]): IvoryTIO[List[OriginalFactset]] =
    factsets.traverse(fid => recreateFactset(repository, fid))

  def recreateFactset(repository: Repository, factset: FactsetId): IvoryTIO[OriginalFactset] =
    IvoryT.read[ResultTIO] >>= (read => IvoryT.fromResultTIO(for {
      hr         <- repository.asHdfsRepository[IO]
      config     <- Metadata.configuration.toIvoryT(repository).run(read)
      dictionary <- Metadata.latestDictionaryFromIvory(repository)
      factsetPath = hr.toIvoryLocation(Repository.factset(factset)).toHdfsPath
      namespaces <- Namespaces.namespaceSizes(factsetPath).run(hr.configuration)
      partitions <- Hdfs.globFiles(factsetPath, HdfsGlobs.FactsetPartitionsGlob + "/*").filterHidden.run(hr.configuration)
      version    <- Versions.read(hr, factset)
      tmpOut     <- Repository.tmpDir("recreate").map(hr.toIvoryLocation)
      _          <- ResultT.safe[IO, Unit](
                      RecreateFactsetJob.run(hr.configuration
                                           , version
                                           , dictionary
                                           , namespaces
                                           , partitions
                                           , tmpOut.toHdfsPath
                                           , 1.gb // 1GB per reducer
                                           , hr.codec))
      expired     = hr.toIvoryLocation(Repository.tmp("expired", KeyName.unsafe(factset.render + "." + DateTime.now(config.timezone).toString("yyyyMMddhhmmss"))))
      _          <- commitFactset(factsetPath, expired.toHdfsPath, tmpOut.toHdfsPath).run(hr.configuration)
      _          <- IvoryStorage.writeFactsetVersion(hr, List(factset))
    } yield OriginalFactset(factset, expired)))

  def commitFactset(factset: Path, expired: Path, tmp: Path): Hdfs[Unit] = for {
    _ <- Hdfs.mkdir(expired.getParent)
    _ <- Hdfs.mv(factset, expired)
    _ <- Hdfs.mv(tmp, factset)
  } yield ()

}

case class OriginalFactset(factsetId: FactsetId, path: IvoryLocation)
