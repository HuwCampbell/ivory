package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._

class SyncSpec extends Specification with ScalaCheck { def is = s2"""

Helper functions
================
 checkPaths                                       $checkPaths

"""

  def checkPaths = prop((factset: Factset) => {
    val datasets = Datasets(List(Prioritized(Priority.Min, FactsetDataset(factset))))
    Sync.getKeys(datasets).length must be_==(factset.partitions.length)
  })

}
