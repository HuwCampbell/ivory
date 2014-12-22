package com.ambiata.ivory.storage.partition

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.poacher.hdfs._

import org.apache.hadoop.fs.Path
import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._


class PartitionsSpec extends Specification with ScalaCheck { def is = s2"""

  Glob calculation should include all partititons.                   $contains

  Glob calculation should always prefix with factset path.           $prefix

  All globs should return the directory it points to.                 $valid

"""

  def onGlob(run: (HdfsRepository, FactsetId, List[Partition], List[String]) => Boolean) =
    prop((factset: FactsetId, partitions: List[Partition]) =>
      RepositoryBuilder.using(repository => {
        val globs = Partitions.globs(repository, factset, partitions)
        run(repository, factset, partitions, globs).pure[RIO]
      }) must beOkValue(true))

  def contains =
    onGlob((repository, factset, partitions, globs) =>
      partitions.forall(p => globs.exists(_.contains(p.key.name))))

  def prefix =
    onGlob((repository, factset, partitions, globs) =>
      globs.forall(_.startsWith(repository.toIvoryLocation(Repository.factset(factset)).toHdfsPath.toString)))

  def valid =
    prop((factset: FactsetId, partitions: List[Partition]) =>
      RepositoryBuilder.using(repository => for {
        _ <- partitions.traverse(p =>
          Hdfs.mkdir(repository.toIvoryLocation(Repository.factset(factset) / p.key).toHdfsPath)
        ).run(repository.configuration)
        globs = Partitions.globs(repository, factset, partitions)
        e <- globs.traverse(g =>
          Hdfs.filesystem.map(fs => Option(fs.globStatus(new Path(g))).map(!_.isEmpty).getOrElse(false))
        ).run(repository.configuration)
      } yield e.forall(_ === true)) must beOkValue(true))
}
