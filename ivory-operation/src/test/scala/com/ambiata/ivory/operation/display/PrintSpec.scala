package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core.arbitraries.FactsWithDictionary
import com.ambiata.ivory.core.thrift.ThriftFact
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.notion.core.Key

import org.specs2._

import scalaz.effect._

class PrintSpec extends Specification with ScalaCheck { def is = s2"""

 Can stream thrift entities from sequence file        $print

"""

  def print = prop { facts: FactsWithDictionary => (for {
    repo <- RepositoryBuilder.repository
    out   = repo.toIvoryLocation(Key.unsafe("tmp"))
    _    <- RepositoryBuilder.writeFacts(repo.configuration, facts.facts, out.location)
    _     = println(out.location.path)
    sb    = List.newBuilder[ThriftFact]
    _    <- Print.printWith(out.toHdfsPath, repo.configuration, new ThriftFact) {
      f => IO { sb += f.deepCopy; () }
    }} yield sb.result()) must beOkValue(facts.facts.map(_.toThrift))
  }.set(minTestsOk = 3)
}
