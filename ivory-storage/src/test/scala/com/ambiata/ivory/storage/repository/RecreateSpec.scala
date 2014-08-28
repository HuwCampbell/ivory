package com.ambiata.ivory.storage.repository

import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.ivory.core.IvorySyntax._
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.fact.Versions
import com.ambiata.ivory.storage.legacy.SampleFacts
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.testing.ResultTIOMatcher._
import org.apache.hadoop.io.compress._
import org.joda.time.LocalDate
import org.specs2.Specification

class RecreateSpec extends Specification with SampleFacts { def is = s2"""
  recompression of a factset $e1
"""

  def fact(i: Int) = StringFact("eid1", FeatureId(Name("ns1"), "fid1"), Date.fromLocalDate(new LocalDate(2012, 9, i)),  Time(0), "def")

  def e1 =
    RepositoryBuilder.using { from =>
      RepositoryBuilder.using { to =>
        // recreate the factset in the target repository
        val codec: Option[CompressionCodec] = Some(new DefaultCodec)
        for {
          factset <- RepositoryBuilder.createFacts(from, List((1 to 3).map(fact).toList)).map(_.head.factsetIds.head.value)
          _   <- Recreate.copyFactset(sampleDictionary, from, to, codec, 30.mb, false)(factset).run(to.scoobiConfiguration)
          // check that it has been properly created
          _   <- Versions.read(to, factset)
          out <- Hdfs.globFiles(to.factset(factset).toHdfs, "*/*/*/*/*").run(to.configuration)
        } yield out
      }
    } must beOkLike((_: List[_]) must not(beEmpty))

}
