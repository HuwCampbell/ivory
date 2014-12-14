package com.ambiata.ivory.storage.fact

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.version.{Version => V}
import com.ambiata.mundane.control._

import scalaz._, Scalaz._, effect.IO

object Versions {
  def read(repository: Repository, factset: FactsetId): RIO[FactsetVersion] =
    V.read(repository, Repository.factset(factset)).flatMap(parse(factset, _))

  def parse(factset: FactsetId, version: V): RIO[FactsetVersion] =
    FactsetVersion.fromString(version.toString) match {
      case None =>
        ResultT.fail(s"Factset version '${version}' in factset '${factset}' not found.")
      case Some(v) =>
        v.pure[RIO]
    }
}
