package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.store._
import com.ambiata.ivory.storage.metadata.Metadata._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2.Specification

class DictionaryImporterSpec extends Specification { def is = s2"""

 A dictionary can be imported in a ivory repository
   with a dictionary saved as a Path locally               $e1
   with a dictionary when updated                          $e2
   but fails with an invalid dictionary update             $invalidDict
   and succeeds with a forced dictionary update            $invalidDictForced

"""

  import DictionaryImporter._

  val opts = ImportOpts(Override, force = false)

  def e1 = {
    val dictionaryPath = FilePath("dictionary.psv")
    val dictionary = """demo|postcode|string|categorical|Postcode|☠"""

    val dict = Dictionary(Map(FeatureId("demo", "postcode") -> FeatureMeta(StringEncoding, CategoricalType, "Postcode", List("☠"))))
    Temporary.using(dir => for {
      _    <- Streams.write(new java.io.FileOutputStream((dir </> dictionaryPath).toFile), dictionary)
      repo  = Repository.fromLocalPath(dir)
      _    <- fromPath(repo, StorePath(repo.toStore, dictionaryPath), opts.copy(ty = Override))
      out  <- dictionaryFromIvory(repo)
    } yield out).run.unsafePerformIO() must beOkValue(dict)
  }

  def e2 = {
    val dict1 = Dictionary(Map(FeatureId("a", "b") -> FeatureMeta(StringEncoding, CategoricalType, "", Nil)))
    val dict2 = Dictionary(Map(FeatureId("c", "d") -> FeatureMeta(StringEncoding, CategoricalType, "", Nil)))
    Temporary.using { dir =>
      val repo = Repository.fromLocalPath(dir)
      for {
        _ <- fromDictionary(repo, dict1, opts.copy(ty = Override))
        _ <- fromDictionary(repo, dict2, opts.copy(ty = Update))
        out <- dictionaryFromIvory(repo)
      } yield out
    }.run.unsafePerformIO() must beOkValue(dict1.append(dict2))
  }

  def invalidUpgrade(force: Boolean) = {
    val fid = FeatureId("a", "b")
    val dict1 = Dictionary(Map(fid -> FeatureMeta(StringEncoding, CategoricalType, "", Nil)))
    val dict2 = Dictionary(Map(fid -> FeatureMeta(BooleanEncoding, CategoricalType, "", Nil)))
    Temporary.using { dir =>
      val repo = Repository.fromLocalPath(dir)
      fromDictionary(repo, dict1, opts.copy(ty = Override))
        .flatMap(_ => fromDictionary(repo, dict2, opts.copy(ty = Override, force = force)))
    }.run.unsafePerformIO()
  }

  def invalidDict =
    invalidUpgrade(false) must beOkLike(_.isFailure)

  def invalidDictForced =
    invalidUpgrade(true) must beOkLike(_.isSuccess)
}
