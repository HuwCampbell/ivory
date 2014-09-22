package com.ambiata.ivory.storage.lookup

import com.ambiata.mundane.io.BytesQuantity
import com.ambiata.poacher.hdfs.Hdfs
import org.apache.hadoop.fs.Path

object ReducerSize {

  /** For a single sequence input, and given an optimalSize, calculate how many reducers to use */
  def calculate(path: Path, optimalSize: BytesQuantity): Hdfs[Int] =
    Hdfs.size(path).map(size => calculateFromSize(size, optimalSize))

  def calculateFromSize(size: BytesQuantity, optimalSize: BytesQuantity): Int =
    Math.max((size.toBytes.value / optimalSize.toBytes.value).toInt, 1)
}
