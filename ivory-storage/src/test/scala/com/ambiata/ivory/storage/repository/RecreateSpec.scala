package com.ambiata.ivory.storage.repository

import java.io.File

import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.ivory.scoobi.FactFormats._
import com.ambiata.ivory.storage.fact.{FactsetVersion, Versions}
import com.ambiata.ivory.storage.legacy.IvoryStorage._
import com.ambiata.ivory.storage.legacy.{IvoryStorage, SampleFacts}
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.mundane.io.FilePath
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.impl.{ScoobiConfigurationImpl, Configurations}
import com.nicta.scoobi.testing.HadoopSpecificationLike
import org.apache.hadoop.io.compress._
import org.joda.time.LocalDate
import org.specs2.Specification

import scalaz._

class RecreateSpec extends Specification with HadoopSpecificationLike with SampleFacts { def is = s2"""
  recompression of a factset $e1
"""

  def e1 = {
    implicit val sc: ScoobiConfiguration = new ScoobiConfigurationImpl()
    sc.set(Configurations.JOB_STEP, "1")

    // create 2 repositories
    val dir = "target/RecreateSpec"
    val from = HdfsRepository(FilePath(s"$dir/from"), sc.configuration, ScoobiRun(sc))
    val to   = HdfsRepository(FilePath(s"$dir/to"), sc.configuration, ScoobiRun(sc))
    Hdfs.deleteAll(FilePath(s"$dir/from").toHdfs).run(sc.configuration) must beOk
    Hdfs.deleteAll(FilePath(s"$dir/to").toHdfs).run(sc.configuration) must beOk

    // create a dictionary
    val dictionary = Metadata.dictionaryFromIvory(from).run.unsafePerformIO.toOption.getOrElse(createDictionary(from))

    // create a factset in the first repository
    def fact(i: Int) = StringFact("eid1", FeatureId("ns1", "fid1"), Date.fromLocalDate(new LocalDate(2012, 9, i)),  Time(0), "def")
    val facts = fromLazySeq[Fact]((1 to 3).map(i => fact(i)))
    val factset = FactsetId("name")
    facts.toIvoryFactset(from, factset, None)(sc).persist
    IvoryStorage.writeFactsetVersion(from, List(factset)) must beOk

    // recreate the factset in the target repository
    val codec: Option[CompressionCodec] = Some(new DefaultCodec)
    Recreate.copyFactset(dictionary, from, to, codec, 30.mb, false)(factset.name).run(sc) must beOk

    // check that it has been properly created
    Versions.read(to, factset) must beOk
    Hdfs.globFiles(to.factset(factset).toHdfs, "*/*/*/*/*/*").run(sc) must beOkLike((_:List[_]) must not(beEmpty))
  }

}

