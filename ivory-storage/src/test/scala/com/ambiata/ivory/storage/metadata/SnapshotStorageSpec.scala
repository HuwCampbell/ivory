package com.ambiata.ivory.storage.metadata

import com.ambiata.mundane.control._
import com.ambiata.mundane.testing.RIOMatcher._
import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries.Arbitraries._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.storage.manifest._
import com.ambiata.disorder._

import org.specs2._

import scalaz._, Scalaz._

class SnapshotStorageSpec extends Specification with ScalaCheck { def is = s2"""

PropNoShrinkerties
----------

  We should be able to allocate and read a bunch of snapshot ids, reguardless of whether they are used:
    $ids

  We should be able to retrieve a snapshot with complete metadata via id:
    $byId

  We should not fail if trying to retrieve a snapshot without complete metadata, just ignore it:
    $byIdNone

  List all snapshots, ignoring incomplete snapshots:
    $list

"""

  def ids = propNoShrink((n: NaturalInt) => (n.value != 0) ==> { RepositoryBuilder.using(repository => for  {
    allocated <- (1 to n.value).toList.traverse(_ => SnapshotStorage.allocateId(repository))
    read <- SnapshotStorage.ids(repository)
  } yield allocated -> read) must beOkLike({ case (allocated, read) => (read -> read.size) ==== (allocated -> n.value) }) })
    .set(minTestsOk = 5)

  def byId = propNoShrink((dictionary: Dictionary, facts: List[Fact], date: Date) => (!facts.isEmpty) ==> { RepositoryBuilder.using(repository => for {
    c <- RepositoryBuilder.createCommit(repository, dictionary, List(facts))
    id <- SnapshotStorage.allocateId(repository)
    _ <- SnapshotManifest.io(repository, id).write(SnapshotManifest.createLatest(c.id, id, date))
    r <- SnapshotStorage.byIdOrFail(repository, id)
  } yield (id, r, c)) must beOkLike({
    case (id, r, c) => (r.id, r.date, r.store, r.dictionary.map(_.value)) ==== ((id, date, c.store, dictionary.some))
  }) })
    .set(minTestsOk = 5)

  def byIdNone = propNoShrink((dictionary: Dictionary, facts: List[Fact], date: Date) => (!facts.isEmpty) ==> { RepositoryBuilder.using(repository => for {
    c <- RepositoryBuilder.createCommit(repository, dictionary, List(facts))
    id <- SnapshotStorage.allocateId(repository)
    r <- SnapshotStorage.byId(repository, id)
  } yield r) must beOkLike(_ must beNone) })
    .set(minTestsOk = 5)

  def list = propNoShrink((n: NaturalInt, dictionary: Dictionary, facts: List[Fact], date: Date) => (!facts.isEmpty && n.value != 0) ==> { RepositoryBuilder.using(repository => for {
    c <- RepositoryBuilder.createCommit(repository, dictionary, List(facts))
    _ <- (1 to n.value).toList.traverse(z => for {
        id <- SnapshotStorage.allocateId(repository)
        _ <- RIO.when(z % 2 == 0, SnapshotManifest.io(repository, id).write(SnapshotManifest.createLatest(c.id, id, date)))
      } yield ())
    r <- SnapshotStorage.list(repository)
  } yield r) must beOkLike(r => r.size ==== (n.value / 2)) })
    .set(minTestsOk = 5)
}
