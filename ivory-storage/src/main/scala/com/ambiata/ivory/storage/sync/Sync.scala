package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.notion.core._

object Sync {
   val ChunkSize = 1024000

   def getKeys(data: Datasets): List[Key] = data.sets.flatMap(_.value match {
     case FactsetDataset(f) =>
       f.partitions.map(Repository.factset(f.id) / _.key)
     case SnapshotDataset(s) =>
       Repository.snapshot(s.id) :: Nil
   })
}
