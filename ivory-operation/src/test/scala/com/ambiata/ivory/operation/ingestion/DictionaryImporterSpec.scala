package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.operation.ingestion.DictionaryImporter._
import com.ambiata.ivory.core.Arbitraries.DictionaryArbitrary
import com.ambiata.ivory.storage.Arbitraries.StoreTypeArbitrary
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store.{Key, PosixStore}
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.specs2.{ScalaCheck, Specification}
import org.specs2.matcher.ThrownExpectations
import scalaz.syntax.bind._


class DictionaryImporterSpec extends Specification with ThrownExpectations with ScalaCheck { def is = s2"""

 A dictionary can be imported in a ivory repository
   with a dictionary saved as a Path locally               $local
   with a dictionary when updated                          $updated
   but fails with an invalid dictionary update             $invalidDict
   and succeeds with a forced dictionary update            $invalidDictForced

 A dictionary can be imported from
   any type of reference to any type of repository         $differentStoreDict      ${tag("aws")}
"""

  val opts = ImportOpts(Override, force = false)

  def local = {

    val dict = Dictionary(List(Definition.concrete(FeatureId(Name("demo"), "postcode"), StringEncoding, Some(CategoricalType), "Postcode", List("☠"))))
    Temporary.using { dir =>
      val dictionaryPath = dir <|> "dictionary.psv"
      for {
        _    <- Streams.write(new java.io.FileOutputStream(dictionaryPath.toFile), DictionaryTextStorageV2.delimitedString(dict))
        repo =  Repository.fromIvoryLocation(LocalIvoryLocation(LocalLocation(dir)), IvoryConfiguration.Empty)
        _    <- DictionaryImporter.importFromPath(repo, IvoryLocation.fromFilePath(dictionaryPath), opts.copy(ty = Override))
        out  <- latestDictionaryFromIvory(repo)
      } yield out
    } must beOkValue(dict)
  }

  def updated = {
    val dict1 = Dictionary(List(Definition.concrete(FeatureId(Name("a"), "b"), StringEncoding, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(FeatureId(Name("c"), "d"), StringEncoding, Some(CategoricalType), "", Nil)))
    Temporary.using { dir =>
      val repo = Repository.fromIvoryLocation(LocalIvoryLocation(LocalLocation(dir)), IvoryConfiguration.Empty)
      for {
        _    <- fromDictionary(repo, dict1, opts.copy(ty = Override))
        _    <- fromDictionary(repo, dict2, opts.copy(ty = Update))
        out  <- latestDictionaryFromIvory(repo)
      } yield out
    }.map(_.byFeatureId) must beOkValue(dict1.append(dict2).byFeatureId)
  }

  def invalidUpgrade(force: Boolean) = {
    val fid = FeatureId(Name("a"), "b")
    val dict1 = Dictionary(List(Definition.concrete(fid, StringEncoding, Some(CategoricalType), "", Nil)))
    val dict2 = Dictionary(List(Definition.concrete(fid, BooleanEncoding, Some(CategoricalType), "", Nil)))
    Temporary.using { dir =>
      val repo = LocalRepository(LocalLocation(dir))
      fromDictionary(repo, dict1, opts.copy(ty = Override))
        .flatMap(_ => fromDictionary(repo, dict2, opts.copy(ty = Override, force = force)))
    }
  }

  def invalidDict =
    invalidUpgrade(false) must beOkLike(r => r._1.isFailure && r._2.isEmpty)

  def invalidDictForced =
    invalidUpgrade(true) must beOkLike(r => r._1.isFailure && r._2.isDefined)

  def differentStoreDict = prop((ivoryType: TemporaryLocations.TemporaryType, dictType: TemporaryLocations.TemporaryType, dict: Dictionary) => {
    TemporaryLocations.withRepository(ivoryType){ivory => for {
      _   <- Repositories.create(ivory)
      _   <- TemporaryLocations.withIvoryLocationFile(dictType){ location =>
         IvoryLocation.writeUtf8(location, DictionaryTextStorageV2.delimitedString(dict)) >>
         importFromPath(ivory, location, opts.copy(ty = Override))
      }
      out <- latestDictionaryFromIvory(ivory)
    } yield out.byFeatureId } must beOkValue(dict.byFeatureId)
  }).set(minTestsOk = 20)
}
