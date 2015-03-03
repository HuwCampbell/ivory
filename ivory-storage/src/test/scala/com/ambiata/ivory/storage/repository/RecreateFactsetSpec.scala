package com.ambiata.ivory.storage.repository

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.FactsWithDictionary
import com.ambiata.ivory.storage.metadata.Metadata
import com.ambiata.ivory.storage.control.IvoryRead
import com.ambiata.mundane.io._
import com.ambiata.mundane.io.Arbitraries._
import com.ambiata.poacher.hdfs._
import org.apache.hadoop.fs.Path
import com.ambiata.mundane.testing.RIOMatcher._
import org.specs2.{ScalaCheck, Specification}

import scalaz._, Scalaz._

class RecreateFactsetSpec extends Specification with ScalaCheck { def is = s2"""

Recreate Factset Tests
-----------------------

  Can recreate factsets                  $recreate ${tag("mr")}
  CommitFactset works                    $commit

"""

  def recreate = prop((facts1: FactsWithDictionary, facts2: FactsWithDictionary) =>
    RepositoryBuilder.using(repo => for {
      storeId   <- RepositoryBuilder.createRepo(repo, facts1.dictionary.append(facts2.dictionary), List(facts1.facts, facts2.facts))
      // Remove part of the dictionary to test that we don't fail
      _         <- Metadata.dictionaryToIvory(repo, facts2.dictionary)
      store     <- Metadata.featureStoreFromIvory(repo, storeId)
      recreated <- RecreateFactset.recreateFactsets(repo, store.unprioritizedIds).run.run(IvoryRead.create)
      completed <- recreated.completed.traverse(re => Hdfs.exists(re.path.toHdfsPath).map((re.factsetId, _))).run(repo.configuration)
    } yield (completed, recreated.incompleted)) must beOkValue(((List(FactsetId.initial, FactsetId.initial.next.get).map((_, true)), Nil)))
  ).set(minTestsOk = 3)

  def commit = prop((local: LocalTemporary) => for {
    base <- local.directory
    conf <- ConfigurationTemporary(base.path).conf
    factset = new Path((base </> "factsets" </> "orig.factset" </> "data").path)
    tmp = new Path((base </> "tmp" </> "new.factset" </> "data").path)
    expired = new Path((base </> "tmp" </> "expided.factsets").path)
    r <- (for {
      _ <- Hdfs.write(factset, "old")
      _ <- Hdfs.write(tmp, "new")
      _ <- RecreateFactset.commitFactset(factset.getParent, expired, tmp.getParent)
      n <- Hdfs.readContentAsString(factset)
      o <- Hdfs.readContentAsString(expired)
    } yield n -> o).run(conf)
  } yield r ==== "new" -> "old")
}
