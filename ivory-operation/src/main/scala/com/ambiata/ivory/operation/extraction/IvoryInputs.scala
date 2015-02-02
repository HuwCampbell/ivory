package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
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
  , datasets: Datasets
  , factset: Class[_ <: CombinableMapper[_, _, _, _]]
  , snapshot: Class[_ <: CombinableMapper[_, _, _, _]]
  ): Unit = {
    val summary = datasets.summary
    println(s"""Configuring mapreduce job, with ${summary.partitions} partitions, ${summary.snapshot.map(s => s"and snapshot ${s.render}, ").getOrElse("")} totalling ${summary.bytes}""")
    val factsets = InputSpecification(classOf[SequenceFileInputFormat[_, _]], factset, datasets.sets.flatMap(p => p.value match {
      case FactsetDataset(factset) =>
        Partitions.globs(repository, factset.id, factset.partitions.map(_.value)).map(f => new Path(f))
      case SnapshotDataset(snapshot) =>
        Nil
    }))
    val snapshots = InputSpecification(classOf[SequenceFileInputFormat[_, _]], snapshot, datasets.sets.flatMap(p => p.value match {
      case FactsetDataset(factset) =>
        Nil
      case SnapshotDataset(snapshot) =>
        List(repository.toIvoryLocation(Repository.snapshot(snapshot.id)).toHdfsPath)
    }))
    ProxyInputFormat.configure(context, job, List(factsets, snapshots))
  }

}
