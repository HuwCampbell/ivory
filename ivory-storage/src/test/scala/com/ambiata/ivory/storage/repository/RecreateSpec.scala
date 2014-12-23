package com.ambiata.ivory.storage.repository

import com.ambiata.poacher.hdfs.Hdfs
import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.legacy.SampleFacts
import com.ambiata.mundane.io.MemoryConversions._
import com.ambiata.mundane.testing.RIOMatcher._
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
          _       <- RepositoryBuilder.createDictionary(from, sampleDictionary)
          factset <- RepositoryBuilder.createFactset(from, (1 to 3).map(fact).toList)
          _       <- Recreate.copyFactset(sampleDictionary, from, to, codec, 30.mb, false)(factset).run(to.scoobiConfiguration)
          out     <- Hdfs.globFiles(to.toIvoryLocation(Repository.factset(factset)).toHdfsPath, "*/*/*/*/*").run(to.configuration)
        } yield out
      }
    } must beOkLike((_: List[_]) must not(beEmpty))

}
