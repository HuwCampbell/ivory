package com.ambiata.ivory.storage.fact

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, IntWritable, NullWritable}

trait MrSnapshotFactFormatV1 extends MrFactFormat[NullWritable, BytesWritable] {
  def factConverter(path: Path): MrFactConverter[NullWritable, BytesWritable] =
    MutableFactConverter()
}

object MrSnapshotFactFormatV1 extends MrSnapshotFactFormatV1

trait MrSnapshotFactFormatV2 extends MrFactFormat[IntWritable, BytesWritable] {
  def factConverter(path: Path): MrFactConverter[IntWritable, BytesWritable] =
    NamespaceDateFactConverter(Namespaces.fromSnapshotPath(path))
}

object MrSnapshotFactFormatV2 extends MrSnapshotFactFormatV2
