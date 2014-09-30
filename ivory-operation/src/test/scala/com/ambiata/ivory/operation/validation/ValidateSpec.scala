package com.ambiata.ivory.operation.validation

import com.ambiata.ivory.core._, Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.metadata._
import com.ambiata.mundane.control.ResultT
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.ambiata.mundane.io._
import com.nicta.scoobi.Scoobi._
import org.specs2._
import org.specs2.matcher.{ThrownExpectations, FileMatchers}
import scalaz.effect.IO

class ValidateSpec extends Specification with ThrownExpectations with FileMatchers with ScalaCheck { def is = s2"""

  Can validate with correct encoding                     $valid
  Can validate with incorrect encoding                   $invalid

  Validate feature store                                 $featureStore
  Validate fact set                                      $factSet

  """

  def valid = prop((e: EncodingAndValue) =>
    Validate.validateEncoding(e.value, e.enc).toEither must beRight
  )

  def invalid = prop((e: EncodingAndValue, e2: Encoding) => (e.enc != e2 && !isCompatible(e, e2)) ==> {
    Validate.validateEncoding(e.value, e2).toEither must beLeft
  })


  def featureStore = {
    RepositoryBuilder.using { repo =>
      val outpath = repo.root </> "out"
      val dict = Dictionary(List(
        Definition.concrete(FeatureId(Name("ns1"), "fid1"), DoubleEncoding, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns2"), "fid3"), BooleanEncoding, Some(CategoricalType), "desc", Nil))
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
        Definition.concrete(FeatureId(Name("ns1"), "fid1"), DoubleEncoding, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns1"), "fid2"), IntEncoding, Some(NumericalType), "desc", Nil),
        Definition.concrete(FeatureId(Name("ns2"), "fid3"), BooleanEncoding, Some(CategoricalType), "desc", Nil)
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

  // A small subset of  encoded values are valid for different optional/empty Structs/Lists
  private def isCompatible(e1: EncodingAndValue, e2: Encoding): Boolean =
    (e1, e2) match {
      case (EncodingAndValue(_, StructValue(m)), StructEncoding(v)) => m.isEmpty && v.forall(_._2.optional)
      case (EncodingAndValue(_, ListValue(l)), ListEncoding(e))     => l.forall(v => isCompatible(EncodingAndValue(e, v), e))
      case _ => false
    }
}
