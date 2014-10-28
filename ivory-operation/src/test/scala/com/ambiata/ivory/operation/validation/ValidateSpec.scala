package com.ambiata.ivory.operation.validation

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control.ResultT
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import org.specs2.matcher.{ThrownExpectations, FileMatchers}
import scalaz.effect.IO

class ValidateSpec extends Specification with ThrownExpectations with FileMatchers { def is = s2"""

  Validate feature store                                 $featureStore
  Validate fact set                                      $factSet

  """

  def featureStore = {
    RepositoryBuilder.using { repo =>
      val outpath = repo.root </> "out"
      val dict = Dictionary(List(
        Definition.concrete(FeatureId(Name("ns1"), "fid1"), DoubleEncoding, Mode.State, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Mode.State, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns2"), "fid3"), BooleanEncoding, Mode.State, Some(CategoricalType), "desc", Nil))
      )

      val facts1 = List(
        StringFact("eid1", FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
        IntFact("eid1", FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
        BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true)
      )
      val facts2 = List(
        StringFact("eid1",  FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "def")
      )

      implicit val sc = repo.scoobiConfiguration
      for {
        fsid <- RepositoryBuilder.createFacts(repo, List(facts1, facts2)).map(_._1)
        fs   <- Metadata.featureStoreFromIvory(repo, fsid)
        _    <- ValidateStoreHdfs(repo, fs, dict, false).exec(outpath.toHdfsPath).run(sc)
        res  <- ResultT.ok[IO, List[String]](fromTextFile(outpath.toHdfs).run.toList)
      } yield (res, fsid)
    } must beOkLike { case (res, fsid) =>
      res must have size(1)
      res must contain("Not a valid double!")
      res must contain("eid1")
      res must contain("ns1")
      res must contain("fid1")
      res must contain(fsid.render)
    }
  }

  def factSet = {
    RepositoryBuilder.using { repo =>
      val outpath = repo.root </> "out"

      val dict = Dictionary(List(
        Definition.concrete(FeatureId(Name("ns1"), "fid1"), DoubleEncoding, Mode.State, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Mode.State, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns2"), "fid3"), BooleanEncoding, Mode.State, Some(CategoricalType), "desc", Nil)
      ))

      val facts1 = List(
        StringFact("eid1", FeatureId(Name("ns1"), "fid1"), Date(2012, 10, 1), Time(0), "abc"),
        IntFact("eid1", FeatureId(Name("ns1"), "fid2"), Date(2012, 10, 1), Time(0), 10),
        BooleanFact("eid1", FeatureId(Name("ns2"), "fid3"), Date(2012, 3, 20), Time(0), true)
      )

      implicit val sc = repo.scoobiConfiguration
      for {
        fsid <- RepositoryBuilder.createFactset(repo, facts1)
        _    <- ValidateFactSetHdfs(repo, fsid, dict).exec(outpath.toHdfsPath).run(sc)
        res  <- ResultT.ok[IO, List[String]](fromTextFile(outpath.toHdfs).run.toList)
      } yield res
    } must beOkLike { res =>
      res must have size(1)
      res must contain("Not a valid double!")
      res must contain("eid1")
      res must contain("ns1")
      res must contain("fid1")
    }
  }
}
