package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core.FactsetFormat

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.fs.Path

trait MrFactsetFactFormatV1 extends MrFactFormat[NullWritable, BytesWritable] {
  val format: FactsetFormat = FactsetFormat.V1
  def factConverter(path: Path): MrFactConverter[NullWritable, BytesWritable] =
    PartitionFactConverter(FactsetInfo.getBaseInfo(path)._2)
}

object MrFactsetFactFormatV1 extends MrFactsetFactFormatV1

trait MrFactsetFactFormatV2 extends MrFactFormat[NullWritable, BytesWritable] {
  val format: FactsetFormat = FactsetFormat.V2
  def factConverter(path: Path): MrFactConverter[NullWritable, BytesWritable] =
    PartitionFactConverter(FactsetInfo.getBaseInfo(path)._2)
}

object MrFactsetFactFormatV2 extends MrFactsetFactFormatV2
