package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact._
import com.ambiata.ivory.storage.partition._
import com.ambiata.ivory.mr._
import com.ambiata.poacher.mr._

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

object IvoryInputs {
  def configure(
    context: MrContext
  , job: Job
  , repository: HdfsRepository
  , inputs: List[Prioritized[FactsetGlob]]
  , incremental: Option[Path]
  , factset: Class[_ <: CombinableMapper[_, _, _, _]]
  , snapshot: Class[_ <: CombinableMapper[_, _, _, _]]
  ): Unit = {
    val factsets = inputs.map(_.value.factset).distinct.size
    val partitions = inputs.map(_.value.partitions.size).sum
    println(s"Adding input path for ${partitions} partitions across ${factsets} factsets.")
    incremental.foreach(p => println(s"Adding input path snapshot at path ${p}."))
    val specifications = InputSpecification(classOf[SequenceFileInputFormat[_, _]], factset, for {
        i <- inputs
        g <- Partitions.globs(repository, i.value.factset, i.value.partitions)
      } yield new Path(g)) :: incremental.map(i => InputSpecification(classOf[SequenceFileInputFormat[_, _]], snapshot, i :: Nil)).toList
    ProxyInputFormat.configure(context, job, specifications)
  }
}
