package com.ambiata.ivory.storage.partition

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.ResultTIOMatcher._

import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._


class PartitionsSpec extends Specification with ScalaCheck { def is = s2"""

  Glob calculation should include all partititons.             $contains


  Glob calculation should always prefix with factset path.     $prefix

"""

  def contains =
    onGlob((repository, factset, partitions, globs) =>
      partitions.forall(p => globs.exists(_.contains(p.key.name))))

  def prefix =
    onGlob((repository, factset, partitions, globs) =>
      globs.forall(_.startsWith(repository.toIvoryLocation(Repository.factset(factset)).toHdfsPath.toString)))

  def onGlob(run: (HdfsRepository, FactsetId, List[Partition], List[String]) => Boolean) =
    prop((factset: FactsetId, partitions: List[Partition]) =>
      RepositoryBuilder.using(repository => {
        val globs = Partitions.globs(repository, factset, partitions)
        run(repository, factset, partitions, globs).pure[ResultTIO]
      }) must beOkValue(true))
}
