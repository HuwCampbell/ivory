package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core.TemporaryLocations.Posix
import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.ingestion.DictionaryImporter._
import com.ambiata.ivory.core.Arbitraries.DictionaryArbitrary
import com.ambiata.ivory.storage.Arbitraries.StoreTypeArbitrary
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.notion.core.{Key, PosixStore}
import com.ambiata.mundane.testing.ResultTIOMatcher._
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

  def local = {

    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("demo"), "postcode"), StringEncoding, Some(CategoricalType), "Postcode", List("☠"))))
    TemporaryDirPath.withDirPath { dir =>
      val repository = Repository.fromIvoryLocation(LocalIvoryLocation.create(dir), IvoryConfiguration.Empty)
      val dictionaryPath = dir <|> "dictionary.psv"

      for {
        _    <- Streams.write(new java.io.FileOutputStream(dictionaryPath.toFile), DictionaryTextStorageV2.delimitedString(dict))
        _    <- DictionaryImporter.importFromPath(repository, IvoryLocation.fromFilePath(dictionaryPath), opts.copy(ty = Override))
        out  <- latestDictionaryFromIvory(repository)
      } yield out
    } must beOkValue(dict)
  }

  def updated = {
    val dict1 = Dictionary(List(Definition.concrete(FeatureId(Name("a"), "b"), StringEncoding, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(FeatureId(Name("c"), "d"), StringEncoding, Some(CategoricalType), "", Nil)))
    TemporaryDirPath.withDirPath { dir =>
      val repository = Repository.fromIvoryLocation(LocalIvoryLocation.create(dir), IvoryConfiguration.Empty)
      for {
        _    <- fromDictionary(repository, dict1, opts.copy(ty = Override))
        _    <- fromDictionary(repository, dict2, opts.copy(ty = Update))
        out  <- latestDictionaryFromIvory(repository)
      } yield out
    }.map(_.byFeatureId) must beOkValue(dict1.append(dict2).byFeatureId)
  }

  def invalidUpgrade(force: Boolean) = {
    val fid = FeatureId(Name("a"), "b")
    val dict1 = Dictionary(List(Definition.concrete(fid, StringEncoding, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(fid, BooleanEncoding, Some(CategoricalType), "", Nil)))
    TemporaryDirPath.withDirPath { dir =>
      val repo = LocalRepository.create(dir)
      fromDictionary(repo, dict1, opts.copy(ty = Override))
        .flatMap(_ => fromDictionary(repo, dict2, opts.copy(ty = Override, force = force)))
    }
  }

  def invalidDict =
    invalidUpgrade(false) must beOkLike(r => r._1.isFailure && r._2.isEmpty)

  def invalidDictForced =
    invalidUpgrade(true) must beOkLike(r => r._1.isFailure && r._2.isDefined)

  def differentStoreDict = prop((ivoryType: TemporaryLocations.TemporaryType, dictType: TemporaryLocations.TemporaryType, dict: Dictionary) => {
    TemporaryLocations.withRepository(ivoryType) { ivory => for {
      _   <- Repositories.create(ivory)
      _   <- TemporaryLocations.withIvoryLocationFile(dictType) { location =>
               IvoryLocation.writeUtf8(location, DictionaryTextStorageV2.delimitedString(dict)) >>
               importFromPath(ivory, location, opts.copy(ty = Override))
             }
      out <- latestDictionaryFromIvory(ivory)
    } yield out.byFeatureId } must beOkValue(dict.byFeatureId)
  }).set(minTestsOk = 20)


  def dictionaryDirectory = {
    val dict1 = Dictionary(List(Definition.concrete(FeatureId(Name("a"), "b"), StringEncoding, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(FeatureId(Name("c"), "d"), StringEncoding, Some(CategoricalType), "", Nil)))

    TemporaryLocations.withRepository(Posix) { ivory => for {
      _   <- Repositories.create(ivory)
      _   <- TemporaryLocations.withIvoryLocationDir(Posix) { location =>
               IvoryLocation.writeUtf8(location </> "dict1.psv", DictionaryTextStorageV2.delimitedString(dict1)) >>
               IvoryLocation.writeUtf8(location </> "dict2.psv", DictionaryTextStorageV2.delimitedString(dict2)) >>
               importFromPath(ivory, location, opts.copy(ty = Override))
      }
      out <- latestDictionaryFromIvory(ivory)
    } yield out.byFeatureId } must beOkValue(dict1.append(dict2).byFeatureId)
  }
}
