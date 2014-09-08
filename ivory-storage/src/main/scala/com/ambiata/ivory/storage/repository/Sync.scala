package com.ambiata.ivory.storage.repository

import java.util.UUID

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.io
import com.ambiata.mundane.io._

import com.ambiata.mundane.control._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.fs.{FileSystem, Path}

import scalaz._, Scalaz._, effect.IO, effect.Effect._

object Sync {

  def syncInputDataSet(input: InputDataset, cluster: Cluster): ResultTIO[ShadowInputDataset] = {
    val outputPath: FilePath = FilePath(s"shadowInputDataset/${UUID.randomUUID()}")
    input.location match {
      case LocalLocation(_) => input.path.toFile.isFile match {
        case true =>  localToHdfs(input.path, outputPath, cluster) >> shadowInputDatasetFromPath( outputPath </> input.path.basename, cluster)
        case false => localToHdfs(input.path, outputPath, cluster) >> shadowInputDatasetFromPath(outputPath, cluster)
      }
      case S3Location(b,k)  => s3toHdfs(b, FilePath(k), outputPath, cluster) >> shadowInputDatasetFromPath(outputPath, cluster)
      case HdfsLocation(_)  => ResultT.fail("")
    }
  }

  def shadowInputDatasetFromPath(path: FilePath, cluster: Cluster): ResultTIO[ShadowInputDataset] =
    ResultT.ok(ShadowInputDataset(cluster.root </> path, cluster.configuration.configuration))

  def syncOutputDataSet(input: ShadowOutputDataset, cluster: Cluster, output: OutputDataset): ResultTIO[Unit] =
    output.location match {
      case LocalLocation(_) => hdfsToLocal(input.location, cluster, output.path )
      case S3Location(b,k)  => ResultT.unit
      case HdfsLocation(_)  => ResultT.unit
    }

  def syncToCluster(datasets:Datasets, source: Repository, cluster: Cluster): ResultTIO[ShadowRepository] = {
    (source match {
      case S3Repository(bucket, root, _) => getPaths(datasets).traverseU(z => s3toHdfs(bucket, root, z, cluster)).void
      case LocalRepository(root) => getPaths(datasets).traverseU(z => localToHdfs(root, FilePath(""), cluster)).void
      case HdfsRepository(_, _) => ResultT.unit[IO]
    }) >> (source match {
      case HdfsRepository(_,_)   => ResultT.ok(ShadowRepository.fromRepoisitory(source))
      case _                     => ResultT.ok(ShadowRepository(cluster.root, cluster.configuration))
    })
  }

  def syncToRepository(data:Datasets, cluster: Cluster, repo: Repository): ResultTIO[Unit] = repo match {
    case HdfsRepository(_, _)          => ResultT.unit
    case S3Repository(bucket, root, _) => ResultT.unit
    case LocalRepository(root)         => getPaths(data).traverseU(z => hdfsToLocal(cluster.root </> z, cluster, repo.root)).void
  }

  def localToHdfs(base: FilePath, destination: FilePath, cluster: Cluster): ResultTIO[Unit] = for {
    files <- Directories.list(base).map(_.map(_.absolute))
    _     <- files.traverseU({ path =>
      ResultT.using(path.absolute.toInputStream) { input =>
        Hdfs.writeWith(new Path((cluster.root </> destination </> normalise(path, base)).path), { output =>
          Streams.pipe(input, output)  //TODO optimize byte size for streams
        }).run(cluster.hdfsConfiguration)
      }
    })
  } yield ()

  def hdfsToLocal(absoluteBasePath: FilePath, cluster: Cluster, destination: FilePath): ResultTIO[Unit] = for {
    files <- Hdfs.globFilesRecursively(absoluteBasePath.toHdfs).map(_.map(k => FileSystem.get(cluster.hdfsConfiguration).makeQualified(k))).run(cluster.hdfsConfiguration) // sort?
    _     <- files.traverseU({ path =>
      Hdfs.readWith(path, { input => {
        val p = destination </> normalise(FilePath(path.toUri.getPath), cluster.root)
        io.Directories.mkdirs(p.dirname) >> ResultT.using(p.absolute.toOutputStream) { output =>
          Streams.pipe(input, output) //TODO optimize byte size for streams
        }}
      }).run(cluster.hdfsConfiguration)
    })
  } yield ()

  def hdfsToHdfs(data:Datasets, source: ShadowRepository, destination: Repository): Repository =
    ShadowRepository.toRepository(source)

  /**
   * Copy from (bucket, root) => cluster.root </> output
   */
  def s3toHdfs(bucket:String, root: FilePath, output:FilePath, cluster: Cluster): ResultTIO[Unit] =
    ???

  def hdfsToS3(data:Datasets, source: ShadowRepository, destination: Repository): Repository =
    ???

  def getPaths(data:Datasets): List[FilePath] = {
    data.sets.flatMap(_.value match {
      case FactsetDataset(fid, parts) => parts.map(Repository.factset(fid) </> _.path)
      case SnapshotDataset(sid)       => (Repository.snapshots </> sid.render) :: Nil
    })
  }

  def normalise(file: FilePath, base: FilePath): String = {
    if (file.absolute.toFile.equals(base.absolute.toFile)) file.basename.path
    else file.absolute.path.replace(base.absolute.path + "/", "")
  }
}
