package com.ambiata.ivory.storage.metadata

import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.notion.core._

import org.specs2._

import scalaz._, Scalaz._

class FeatureIdMappingsStorageSpec extends Specification with ScalaCheck { def is = s2"""

FeatureId Mappings Storage Spec
-------------------------------

  Symmetric read / write with Key                              $symmetricKey
  Symmetric read / write with IvoryLocation                    $symmetricIvoryLocation
  Symmetric to / from bytes                                    $symmetricBytes
  Persist mappings from Dictionary                             $persistFromDictionary

"""
  val key: Key = Repository.root / FeatureIdMappingsStorage.keyname

  def symmetricKey = propNoShrink((mappings: FeatureIdMappings) =>
    (for {
      repo <- RepositoryBuilder.repository
      _    <- FeatureIdMappingsStorage.toKeyStore(repo, key, mappings)
      m    <- FeatureIdMappingsStorage.fromKeyStore(repo, key)
    } yield m.featureIds) must beOkValue(mappings.featureIds)
  ).set(minTestsOk = 20)

  def symmetricIvoryLocation = propNoShrink((mappings: FeatureIdMappings) =>
    (for {
      repo <- RepositoryBuilder.repository
      loc   = repo.toIvoryLocation(key)
      _    <- FeatureIdMappingsStorage.toIvoryLocation(loc, mappings)
      m    <- FeatureIdMappingsStorage.fromIvoryLocation(loc)
    } yield m.featureIds) must beOkValue(mappings.featureIds)
  ).set(minTestsOk = 20)

  def symmetricBytes = prop((mappings: FeatureIdMappings) => {
    val bytes: Array[Byte] = FeatureIdMappingsStorage.featureIdMappingsToBytes(mappings)
    FeatureIdMappingsStorage.bytesToFeatureIdMappings(bytes).map(_.featureIds) ==== mappings.featureIds.right
  })

  def persistFromDictionary = prop((dictionary: Dictionary) => {
    val base = Repository.root / "mapping-test"
    val expected = FeatureIdMappings.fromDictionary(dictionary).featureIds
    (for {
      repo      <- RepositoryBuilder.repository
      mappings  <- FeatureIdMappingsStorage.fromDictionaryAndSave(repo, base, dictionary)
      mappings2 <- FeatureIdMappingsStorage.fromKeyStore(repo, base / FeatureIdMappingsStorage.keyname)
    } yield (mappings.featureIds, mappings2.featureIds)) must beOkValue((expected, expected))
  }).set(minTestsOk = 20)
}
