package com.ambiata.ivory.core

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import org.apache.hadoop.conf.Configuration
import com.nicta.scoobi.Scoobi._
import scalaz._, effect.IO

object TemporaryIvoryConfiguration {
  def withConf[A](f: IvoryConfiguration => ResultTIO[A]): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    runWithConf(dir, f)
  }

  def runWithConf[A](dir: DirPath, f: IvoryConfiguration => ResultTIO[A]): ResultTIO[A] = {
    val sc = ScoobiConfiguration()
    sc.set("hadoop.tmp.dir", dir.path)
    sc.set("scoobi.dir", dir.path + "/")
    val conf = IvoryConfiguration.fromScoobiConfiguration(sc)
    f(conf)
  }

  def withConfX[A](f: IvoryConfiguration => A): ResultTIO[A] = TemporaryDirPath.withDirPath { dir =>
    withConf(conf => ResultT.ok[IO, A](f(conf)))
  }
}
