package com.ambiata.ivory.storage.fact

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable

trait MrFactFormat[K <: Writable, V <: Writable] {
  def factConverter(path: Path): MrFactConverter[K, V]
}
