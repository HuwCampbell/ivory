package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.Scoobi._
import scalaz._, Scalaz._, effect.IO

object TemporaryIvoryConfiguration {

  def getConf: ResultTIO[IvoryConfiguration] = TemporaryDirPath.withDirPath { dir =>
    val sc = ScoobiConfiguration()
    sc.set("hadoop.tmp.dir", dir.path)
    sc.set("scoobi.dir", dir.path + "/")
    IvoryConfiguration.fromScoobiConfiguration(sc).pure[ResultTIO]
  }

  def withConf[A](f: IvoryConfiguration => ResultTIO[A]): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    getConf >>= f
  }

  def withConfX[A](f: IvoryConfiguration => A): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    withConf(conf => ResultT.ok[IO, A](f(conf)))
  }
}
