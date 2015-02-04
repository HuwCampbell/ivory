package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core.arbitraries.FactsWithDictionary
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.Key
import com.ambiata.poacher.mr.{ThriftSerialiser, Writables}

import org.apache.hadoop.io.NullWritable

import org.specs2._

import scalaz.effect._

class PrintSpec extends Specification with ScalaCheck { def is = s2"""

 Can stream thrift entities from sequence file        $print

"""

  def print = prop((facts: FactsWithDictionary) => {
    val serialiser = ThriftSerialiser()
    (for {
      repo <- RepositoryBuilder.repository
      out   = repo.toIvoryLocation(Key.unsafe("tmp"))
      _    <- RepositoryBuilder.writeFacts(repo.configuration, facts.facts, out.location)
      _     = println(out.location.path)
      sb    = List.newBuilder[ThriftFact]
      _    <- Print.printWith(out.toHdfsPath, repo.configuration, NullWritable.get, Writables.bytesWritable(4096)) {
        (_, bw) => IO { sb += serialiser.fromBytesUnsafe(new ThriftFact, bw.copyBytes); () }
      }
    } yield sb.result()) must beOkValue(facts.facts.map(_.toThrift))
  }).set(minTestsOk = 3)
}
