package com.ambiata.ivory.operation.debug

import com.ambiata.mundane.control._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.metadata._
import org.apache.hadoop.fs.Path
import scalaz.effect.IO

/**
 * Details of a debug-dump-facts request.
 *  - 'factsets' is the list of factsets to include.
 *  - 'snapshots' is the list of snapshots to include.
 *  - 'entities' is the list of entities to filter by, an empty list implies all entities.
 *  - 'attributes' is the list of attributes to filter by, an empty list implies all attributes.
 */
case class DumpFactsRequest(
  factsets: List[FactsetId]
, snapshots: List[SnapshotId]
, entities: List[String]
, attributes: List[String]
)

object DumpFacts {
  def dump(repository: Repository, request: DumpFactsRequest, location: IvoryLocation): ResultT[IO, Unit] = for {
    output     <- location.asHdfsIvoryLocation[IO]
    hdfs       <- repository.asHdfsRepository[IO]
    dictionary <- Metadata.latestDictionaryFromIvory(repository)
    _          <- DumpFactsJob.run(hdfs, dictionary, request, output.toHdfsPath, hdfs.root.codec)
  } yield ()
}
