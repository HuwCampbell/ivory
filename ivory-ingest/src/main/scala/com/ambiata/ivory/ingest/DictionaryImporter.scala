package com.ambiata.ivory.ingest

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.saws.core._
import com.ambiata.saws._
import com.ambiata.ivory.alien.hdfs.HdfsS3Action._
import com.ambiata.mundane.io.FilePath

// FIX move to com.ambiata.ivory.ingest.internal
object DictionaryImporter {

   def onHdfs(repoPath: Path, dictPath: Path, name: String): Hdfs[Unit] = {
     val repo = Repository.fromHdfsPath(repoPath)
     for {
       files <- Hdfs.globFiles(dictPath)
       _     <- if (files.isEmpty) Hdfs.fail(s"Path $dictPath does not exist or has no files!") else Hdfs.ok(())
       ds    <- files.traverse(f => DictionaryTextStorage.dictionaryFromHdfs(f))
       _     <- IvoryStorage.dictionariesToIvory(repo, ds, name)
     } yield ()
   }

  def onS3(path: FilePath, dictionaryName: String, dictionaryPath: FilePath): HdfsS3Action[Dictionary] =
    onS3(Repository.fromS3(path), dictionaryName, dictionaryPath)

  def onS3(repository: S3Repository, dictionaryName: String, dictionaryPath: FilePath): HdfsS3Action[Dictionary] = {
    val onHdfs = for {
      files <- Hdfs.globFiles(new Path(dictionaryPath.path))
      _     <- if (files.isEmpty) Hdfs.fail(s"Path $dictionaryPath does not exist or has no files!") else Hdfs.ok(())
      ds    <- files.traverse(f => DictionaryTextStorage.dictionaryFromHdfs(f))
    } yield ds

    for {
      ds <- HdfsS3Action.fromHdfs(onHdfs)
      a  <- IvoryStorage.dictionariesToIvory(repository, ds, dictionaryName)
    } yield ds.reduce(_ append _)
  }
}
