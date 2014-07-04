package com.ambiata.ivory.ingest

import scalaz._, Scalaz._
import org.apache.hadoop.fs.Path

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._
import com.ambiata.saws.core._
import com.ambiata.saws._
import com.ambiata.mundane.io._
import com.nicta.scoobi.Scoobi._

// FIX move to com.ambiata.ivory.ingest.internal
object DictionaryImporter {
   def onHdfs(repoPath: Path, dictPath: Path, name: String): Hdfs[Unit] = {
     val repo = Repository.fromHdfsPath(repoPath.toString.toFilePath, ScoobiConfiguration())
     for {
       files <- Hdfs.globFiles(dictPath)
       _     <- if (files.isEmpty) Hdfs.fail(s"Path $dictPath does not exist or has no files!") else Hdfs.ok(())
       ds    <- files.traverse(f => DictionaryTextStorage.dictionaryFromHdfs(f))
       _     <- IvoryStorage.dictionariesToIvory(repo, ds, name)
     } yield ()
   }
}
