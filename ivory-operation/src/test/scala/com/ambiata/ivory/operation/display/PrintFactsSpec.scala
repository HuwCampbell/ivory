package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.FactsWithDictionary
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.ivory.storage.fact.Factsets
import com.ambiata.ivory.operation.extraction.Snapshots
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._

import scalaz._, Scalaz._

class PrintFactsSpec extends Specification with ScalaCheck { def is = s2"""

 Can print a factset to the console                                $factset
 Can print a snapshot to the console                               $snapshot

"""

  def factset = prop { facts: FactsWithDictionary =>
    (for {
      repo <- RepositoryBuilder.repository
      _    <- RepositoryBuilder.createDictionary(repo, facts.dictionary)
      fid  <- RepositoryBuilder.createFactset(repo, facts.facts)
      fs   <- Factsets.factset(repo, fid)
      ret  <- PrintFacts.printFactset(repo, fs, Nil, Nil)
    } yield ret) must beOkValue(().right)
  }.set(minTestsOk = 3)

  def snapshot = prop { facts: FactsWithDictionary =>
    (for {
      repo <- RepositoryBuilder.repository
      _    <- RepositoryBuilder.createDictionary(repo, facts.dictionary)
      fid  <- RepositoryBuilder.createFactset(repo, facts.facts)
      snap <- Snapshots.takeSnapshot(repo, IvoryFlags.default, Date.maxValue)
      ret  <- PrintFacts.printSnapshot(repo, snap, Nil, Nil)
    } yield ret) must beOkValue(().right)
  }.set(minTestsOk = 3)
}
