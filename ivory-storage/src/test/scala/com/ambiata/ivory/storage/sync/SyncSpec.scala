package com.ambiata.ivory.storage.sync

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.sync.Sync._
import com.ambiata.mundane.io._

import org.specs2._

class SyncSpec extends Specification with ScalaCheck { def is = s2"""

Helper functions
================
 checkPaths                                       $checkPaths

 'removeCommonPath'
  will return 'Some' if remove is a prefix of input.            $removeSome
  will return 'Some(input)' if remove is root.                  $removeRoot
  will return 'None' if remove is not a prefix of input.        $removeNone


"""

  def checkPaths = prop((factset: Factset) => {
    val datasets = Datasets(List(Prioritized(Priority.Min, FactsetDataset(factset))))
    getKeys(datasets).length must be_==(factset.partitions.length)
  })

  def removeSome = {
    val dir = DirPath.unsafe("/a/b/c")
    val file = dir </> FilePath.unsafe("d")
    removeCommonPath(file, dir) ==== Some(FilePath.unsafe("d"))
  }

  def removeRoot = {
    val dir = DirPath.unsafe("/")
    val file = dir </> FilePath.unsafe("d")
    removeCommonPath(file, dir) ==== Some(FilePath.unsafe("d"))
  }

  def removeNone = {
    val dir = DirPath.unsafe("/a/b/c")
    val file = FilePath.unsafe("d")
    removeCommonPath(file, dir) ==== None
  }

}
