package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.IvoryLocationTemporary._
import com.ambiata.ivory.core.RepositoryTemporary._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.operation.ingestion.DictionaryImporter._
import com.ambiata.ivory.storage.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.notion.core._
import com.ambiata.notion.core.TemporaryType.Posix
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.ThrownExpectations
import scalaz.syntax.bind._


class DictionaryImporterSpec extends Specification with ThrownExpectations with ScalaCheck { def is = s2"""

 A dictionary can be imported in a ivory repository
   with a dictionary saved as a Path locally               $local
   with a dictionary when updated                          $updated

 The import should
   fail with an invalid dictionary update                  $invalidDict
   but succeeds with a forced dictionary update            $invalidDictForced

 A dictionary can be imported from
   any type of reference to any type of repository         $differentStoreDict      ${tag("aws")}
   a directory containing dictionary files                 $dictionaryDirectory
"""

  val opts = ImportOpts(Override, force = false)

  def local = prop((local: LocalTemporary) => for {
    dir  <- local.directoryThatExists
    conf <- IvoryConfigurationTemporary.random.conf
    dict = Dictionary(List(Definition.concrete(FeatureId(Namespace("demo"), "postcode"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "Postcode", List("☠"))))
    repo = Repository.fromIvoryLocation(LocalIvoryLocation.create(dir), conf)
    path = dir <|> "dictionary.psv"
    _    <- Streams.write(new java.io.FileOutputStream(path.toFile), DictionaryTextStorageV2.delimitedString(dict))
    _    <- DictionaryImporter.importFromPath(repo, IvoryLocation.fromFilePath(path), opts.copy(ty = Override))
    out  <- latestDictionaryFromIvory(repo)
  } yield out ==== dict)

  def updated = prop((local: LocalTemporary) => for {
    dir   <- local.directory
    conf  <- IvoryConfigurationTemporary.random.conf
    dict1 = Dictionary(List(Definition.concrete(FeatureId(Namespace("a"), "b"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "", Nil)))
    dict2 = Dictionary(List(Definition.concrete(FeatureId(Namespace("c"), "d"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "", Nil)))
    repo  = Repository.fromIvoryLocation(LocalIvoryLocation.create(dir), conf)
    _     <- fromDictionary(repo, dict1, opts.copy(ty = Override))
    _     <- fromDictionary(repo, dict2, opts.copy(ty = Update))
    out   <- latestDictionaryFromIvory(repo)
    z     = out.byFeatureId
  } yield z ==== dict1.append(dict2).byFeatureId)

  def invalidUpgrade(force: Boolean) = {
    val fid = FeatureId(Namespace("a"), "b")
    val dict1 = Dictionary(List(Definition.concrete(fid, StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(fid, BooleanEncoding.toEncoding, Mode.State, Some(CategoricalType), "", Nil)))
    LocalTemporary.random.directory >>= { dir =>
      val repo = LocalRepository.create(dir)
      fromDictionary(repo, dict1, opts.copy(ty = Override))
        .flatMap(_ => fromDictionary(repo, dict2, opts.copy(ty = Override, force = force)))
    }
  }

  def invalidDict =
    invalidUpgrade(false) must beOkLike(r => r._1.isFailure && r._2.isEmpty)

  def invalidDictForced =
    invalidUpgrade(true) must beOkLike(r => r._1.isFailure && r._2.isDefined)

  def differentStoreDict = prop((ivoryType: TemporaryType, dictType: TemporaryType, dict: Dictionary) => {
    withRepository(ivoryType) { ivory => for {
      _   <- Repositories.create(ivory, RepositoryConfig.testing)
      _   <- withIvoryLocationFile(dictType) { location =>
               IvoryLocation.writeUtf8(location, DictionaryTextStorageV2.delimitedString(dict)) >>
               importFromPath(ivory, location, opts.copy(ty = Override))
             }
      out <- latestDictionaryFromIvory(ivory)
    } yield out.byFeatureId } must beOkValue(dict.byFeatureId)
  }).set(minTestsOk = 20)


  def dictionaryDirectory = {
    val dict1 = Dictionary(List(Definition.concrete(FeatureId(Namespace("a"), "b"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(FeatureId(Namespace("c"), "d"), StringEncoding.toEncoding, Mode.State, Some(CategoricalType), "", Nil)))

    withRepository(Posix) { ivory => for {
      _   <- Repositories.create(ivory, RepositoryConfig.testing)
      _   <- withIvoryLocationDir(Posix) { location =>
               IvoryLocation.writeUtf8(location </> "dict1.psv", DictionaryTextStorageV2.delimitedString(dict1)) >>
               IvoryLocation.writeUtf8(location </> "dict2.psv", DictionaryTextStorageV2.delimitedString(dict2)) >>
               importFromPath(ivory, location, opts.copy(ty = Override))
      }
      out <- latestDictionaryFromIvory(ivory)
    } yield out.byFeatureId } must beOkValue(dict1.append(dict2).byFeatureId)
  }
}
