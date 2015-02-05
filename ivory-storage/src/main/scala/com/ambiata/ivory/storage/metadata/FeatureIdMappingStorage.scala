package com.ambiata.ivory.storage.metadata

import com.ambiata.ivory.core._
import com.ambiata.ivory.lookup.ThriftFeatureIdMappings

import com.ambiata.notion.core._
import com.ambiata.mundane.control._
import com.ambiata.mundane.io.FileName
import com.ambiata.poacher.mr.ThriftSerialiser

import scodec.bits.ByteVector
import collection.JavaConverters._

import scalaz._, Scalaz._

object FeatureIdMappingsStorage {

  val keyname = KeyName.unsafe(".mappings.thrift")
  val filename = FileName.unsafe(keyname.name)

  def fromKeyStore(repository: Repository, key: Key): RIO[FeatureIdMappings] = for {
    bytes    <- repository.store.bytes.read(key)
    mappings <- RIO.fromDisjunctionString(bytesToFeatureIdMappings(bytes.toArray))
  } yield mappings

  def toKeyStore(repository: Repository, key: Key, mappings: FeatureIdMappings): RIO[Unit] =
    repository.store.bytes.write(key, ByteVector(featureIdMappingsToBytes(mappings)))

  def fromIvoryLocation(location: IvoryLocation): RIO[FeatureIdMappings] = for {
    bytes    <- IvoryLocation.readBytes(location)
    mappings <- RIO.fromDisjunctionString(bytesToFeatureIdMappings(bytes))
  } yield mappings

  def toIvoryLocation(location: IvoryLocation, mappings: FeatureIdMappings): RIO[Unit] =
    IvoryLocation.writeBytes(location, featureIdMappingsToBytes(mappings))

  def bytesToFeatureIdMappings(bytes: Array[Byte]): String \/ FeatureIdMappings = for {
    mappings <- \/.fromTryCatchNonFatal(ThriftSerialiser().fromBytes1(() => new ThriftFeatureIdMappings, bytes))
                  .leftMap(t => s"Could not deserialise bytes - ${t}")
    features <- mappings.getFeatures.asScala.toList.traverseU(FeatureId.parse)
  } yield FeatureIdMappings(features)

  def featureIdMappingsToBytes(mappings: FeatureIdMappings): Array[Byte] =
    ThriftSerialiser().toBytes(new ThriftFeatureIdMappings(mappings.featureIds.map(_.toString).asJava))

  def fromDictionaryAndSave(repository: Repository, base: Key, dictionary: Dictionary): RIO[FeatureIdMappings] = {
     val mappings = FeatureIdMappings.fromDictionary(dictionary)
     FeatureIdMappingsStorage.toKeyStore(repository, base / keyname, mappings).as(mappings)
  }
}
