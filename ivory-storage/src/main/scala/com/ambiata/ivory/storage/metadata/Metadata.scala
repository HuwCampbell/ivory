package com.ambiata.ivory.storage.metadata

import org.apache.hadoop.fs.Path
import com.ambiata.mundane.control._

import com.ambiata.ivory.core._
import com.ambiata.ivory.storage.repository._
import com.ambiata.ivory.alien.hdfs._

object Metadata {

  def dictionaryFromIvory(repo: Repository): ResultTIO[Dictionary] =
    DictionaryThriftStorage(repo).load

  def dictionaryToIvory(repo: Repository, dictionary: Dictionary): ResultTIO[Unit] =
    DictionaryThriftStorage(repo).store(dictionary).map(_ => ())
}
