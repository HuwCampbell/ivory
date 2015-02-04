package com.ambiata.ivory.operation.extraction

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.partition._
import com.ambiata.ivory.mr._
import com.ambiata.poacher.mr._
import com.ambiata.notion.core.Key

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat

import scalaz._

object IvoryInputs {
  def configure(
    context: MrContext
  , job: Job
  , repository: HdfsRepository
  , datasets: Datasets
  , factsetMapper: FactsetFormat => Class[_ <: CombinableMapper[_, _, _, _]]
  , snapshotMapper: SnapshotFormat => Class[_ <: CombinableMapper[_, _, _, _]]
  ): Unit = {
    val summary = datasets.summary
    println(s"""Configuring mapreduce job, with ${summary.partitions} parititons, ${summary.snapshot.map(s => s"and snapshot ${s.render}, ").getOrElse("")} totalling ${summary.bytes}""")

    val factsetSpecifications: List[InputSpecification] =
      datasets.factsets.groupBy(_.format).toList.map({ case (format, factsets) =>
        InputSpecification(classOf[SequenceFileInputFormat[_, _]], factsetMapper(format), factsets.flatMap(f => factsetPaths(repository, f)))
      })

    val snapshotSpecifications: List[InputSpecification] =
      datasets.snapshots.groupBy(_.format).toList.map({ case (format, snapshots) =>
        InputSpecification(classOf[SequenceFileInputFormat[_, _]], snapshotMapper(format), snapshots.flatMap(s => snapshotPaths(repository, s)))
      })

    ProxyInputFormat.configure(context, job, factsetSpecifications ++ snapshotSpecifications)
  }

  def factsetPaths(repository: HdfsRepository, factset: Factset): List[Path] =
    Partitions.globs(repository, factset.id, factset.partitions.map(_.value)).map(f => new Path(f))

  def snapshotPaths(repository: HdfsRepository, snapshot: Snapshot): List[Path] =
    snapshotKeys(snapshot).map(k => repository.toIvoryLocation(k).toHdfsPath)

  def snapshotKeys(snapshot: Snapshot): List[Key] = {
    val base: Key = Repository.snapshot(snapshot.id)
    snapshot.bytes match {
      case -\/(_)  => List(base)
      case \/-(bs) => bs.map(s => base / s.value.asKeyName)
    }
  }
}
