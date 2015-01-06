package com.ambiata.ivory.operation.display

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.core.arbitraries.FactsWithDictionary
import com.ambiata.ivory.storage.repository.RepositoryBuilder
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2._

class PrintFactsSpec extends Specification with ScalaCheck { def is = s2"""

 A sequence file containing facts can be read and printed on the console $print
 Can render a fact                                                       $renderFact

"""

  def print = prop { facts: FactsWithDictionary => (for {
    repo <- RepositoryBuilder.repository
    _    <- RepositoryBuilder.createDictionary(repo, facts.dictionary)
    fs   <- RepositoryBuilder.createFactset(repo, facts.facts)
    _    <- PrintFacts.print(
      List(repo.toIvoryLocation(Repository.factset(fs)).toHdfsPath),
      repo.configuration,
      '|',
      "NA",
      FactsetFormat.V2
    )} yield ()) must beOk
  }.set(minTestsOk = 3)

  def renderFact = prop { (d: Char, t: String, f: Fact) =>
    val s = TextEscaping.split(d, PrintFacts.renderFact(d, t, f))
    (s(0), s.length) ==== ((f.entity, 6))
  }.set(minTestsOk = 10)
}
