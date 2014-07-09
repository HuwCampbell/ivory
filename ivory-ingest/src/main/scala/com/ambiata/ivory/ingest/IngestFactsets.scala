package com.ambiata.ivory.ingest

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.ivory.core._, IvorySyntax._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.scoobi._


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress._
import org.joda.time.DateTimeZone
import com.nicta.scoobi.Scoobi._

import scalaz._, Scalaz._, effect._, Effect._


// FIX this is unpleasent, but is the current super set of what is there, redefine in terms of "fact-set descriptor".

sealed trait IngestFactsets {
  def ingest(dictionary: Dictionary, namespace: String, factsetId: Factset, input: FilePath, timezone: DateTimeZone): ResultT[IO, Unit]
}

case class MapReduceEavtIngestFactsets(conf: ScoobiConfiguration, repository: HdfsRepository, transform: String => String, codec: Option[CompressionCodec]) {

  def ingest(dictionary: Dictionary, namespace: String, factsetId: Factset, input: FilePath, timezone: DateTimeZone): ResultT[IO, Unit] =
    EavtTextImporter.onHdfs(repository, dictionary, factsetId, namespace, new Path(input.path), repository.errors.toHdfs, timezone, codec, transform).run(conf)
}

case class HdfsEavtIngestFactsets(conf: Configuration, repository: HdfsRepository, transform: String => String, codec: Option[CompressionCodec]) {
  def ingest(dictionary: Dictionary, namespace: String, factsetId: Factset, input: FilePath, timezone: DateTimeZone): ResultT[IO, Unit] =
    EavtTextImporter.onHdfsDirect(conf, repository, dictionary, factsetId, namespace, new Path(input.path), repository.errors.toHdfs, timezone, transform, codec)
}
