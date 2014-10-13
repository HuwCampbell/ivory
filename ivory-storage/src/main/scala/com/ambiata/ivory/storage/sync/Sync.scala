package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.plan._
import com.ambiata.notion.core._

object Sync {
   val ChunkSize = 1024000

   def getKeys(data: Datasets): List[Key] = data.sets.flatMap(_.value match {
     case FactsetDataset(fid, parts) => parts.map(Repository.factset(fid) / _.key)
     case SnapshotDataset(sid)       => (Repository.snapshots / sid.asKeyName) :: Nil
   })
}
