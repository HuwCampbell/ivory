package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy._
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

"""

  import DictionaryImporter._

  def e1 = {
    val dictionaryPath = FilePath("dictionary.psv")
    val dictionary = """demo|postcode|string|categorical|Postcode|☠"""

    val dict = Dictionary(Map(FeatureId("demo", "postcode") -> FeatureMeta(StringEncoding, CategoricalType, "Postcode", List("☠"))))
    Temporary.using(dir => for {
      _    <- Streams.write(new java.io.FileOutputStream((dir </> dictionaryPath).toFile), dictionary)
      repo  = Repository.fromLocalPath(dir)
      _    <- fromPath(repo, StorePath(repo.toStore, dictionaryPath), Override)
      out  <- dictionaryFromIvory(repo)
    } yield out).run.unsafePerformIO() must beOkValue(dict)
  }

  def e2 = {
    val dict1 = Dictionary(Map(FeatureId("a", "b") -> FeatureMeta(StringEncoding, CategoricalType, "", Nil)))
    val dict2 = Dictionary(Map(FeatureId("c", "d") -> FeatureMeta(StringEncoding, CategoricalType, "", Nil)))
    Temporary.using { dir =>
      val repo = Repository.fromLocalPath(dir)
      for {
        _ <- fromDictionary(repo, dict1, Override)
        _ <- fromDictionary(repo, dict2, Update)
        out <- dictionaryFromIvory(repo)
      } yield out
    }.run.unsafePerformIO() must beOkValue(dict1.append(dict2))
  }
}
