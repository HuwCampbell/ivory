package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.plan._
import com.ambiata.mundane.io.FilePath

object Sync {
   val ChunkSize = 1024000

   def getPaths(data:Datasets): List[FilePath] = data.sets.flatMap(_.value match {
     case FactsetDataset(fid, parts) => parts.map(Repository.factset(fid) </> _.path)
     case SnapshotDataset(sid)       => (Repository.snapshots </> sid.render) :: Nil
   })

   def normalise(file: FilePath, base: FilePath): String = {
     if (file.absolute.toFile.equals(base.absolute.toFile)) file.basename.path
     else file.absolute.path.replace(base.absolute.path + "/", "")
   }
 }
