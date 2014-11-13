package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.hadoop.MultipleInputs
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.partition._

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

object IvoryInputs {
  def configure(
    job: Job
  , repository: HdfsRepository
  , inputs: List[Prioritized[FactsetGlob]]
  , incremental: Option[Path]
  , factset: Class[_ <: Mapper[_, _, _, _]]
  , snapshot: Class[_ <: Mapper[_, _, _, _]]
  ): Unit = {

    inputs.foreach(pglob =>
      Partitions.globs(repository, pglob.value.factset, pglob.value.partitions).foreach(glob => {
        println(s"Input path: ${glob}")
        MultipleInputs.addInputPath(job, new Path(glob), classOf[SequenceFileInputFormat[_, _]], factset)
      }))

    incremental.foreach(p => {
      println(s"Incremental path: ${p}")
      MultipleInputs.addInputPath(job, p, classOf[SequenceFileInputFormat[_, _]], snapshot)
    })
  }
}
