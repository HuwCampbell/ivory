package com.ambiata.ivory
package core

import com.ambiata.mundane.control._
import com.ambiata.notion.core.Key
import com.ambiata.poacher.hdfs._

import org.apache.hadoop.fs.Path

import scalaz.effect.{Resource, IO}
import scalaz._, Scalaz._

case class TemporaryRepository[R <: Repository](repo: R) {
  def clean: ResultT[IO, Unit] =
    repo.store.deleteAll(Key.Root)
}

object TemporaryRepository {
  implicit def TemporaryRepositoryResource[R <: Repository]: Resource[TemporaryRepository[R]] = new Resource[TemporaryRepository[R]] {
    def close(temp: TemporaryRepository[R]) = temp.clean.run.void // Squelch errors
  }
}

case class TemporaryLocationDir(location: IvoryLocation) {
  def clean: RIO[Unit] = IvoryLocation.deleteAll(location)
}

object TemporaryLocationDir {
  implicit val TemporaryLocationDirResource: Resource[TemporaryLocationDir] = new Resource[TemporaryLocationDir] {
    def close(temp: TemporaryLocationDir) = temp.clean.run.void // Squelch errors
  }
}

case class TemporaryLocationFile(location: IvoryLocation) {
  def clean: RIO[Unit] = IvoryLocation.delete(location)
}

object TemporaryLocationFile {
  implicit val TemporaryLocationFileResource: Resource[TemporaryLocationFile] = new Resource[TemporaryLocationFile] {
    def close(temp: TemporaryLocationFile) = temp.clean.run.void // Squelch errors
  }
}

case class TemporaryCluster(cluster: Cluster) {
  def clean: RIO[Unit] = Hdfs.deleteAll(cluster.root).run(cluster.hdfsConfiguration)
}
object TemporaryCluster {
  implicit val TemporaryClusterResource: Resource[TemporaryCluster] = new Resource[TemporaryCluster] {
    def close(temp: TemporaryCluster) = temp.clean.run.void // Squelch errors
  }
}

case class TemporaryRepositorySetup[R <: Repository](temporaryRepository: TemporaryRepository[R], setup: RIO[Unit]) {
  def clean: RIO[Unit] = temporaryRepository.clean
  def repository: R = temporaryRepository.repo
}

object TemporaryRepositorySetup {
  implicit def TemporaryRepositorySetupResource[R <: Repository]: Resource[TemporaryRepositorySetup[R]] = new Resource[TemporaryRepositorySetup[R]] {
    def close(repository: TemporaryRepositorySetup[R]) = repository.clean.run.void
  }
}
