package com.ambiata.ivory.data

import com.ambiata.mundane.control._
import com.ambiata.mundane.io._
import com.ambiata.mundane.testing.ResultMatcher._
import org.specs2.Specification
import scalaz._, Scalaz._, effect._
import StoreTestUtil._

class StoreDataUtilSpec extends Specification { def is = s2"""

StoreDataUtil
-------------

  List directory                                $listDir
  List root directory                           $listDirRoot
  List directory starting with slash            $listDirAbs
  List empty directory                          $listDirEmpty

"""

  def listDir = run { store =>
    List("a/b/c", "a/b/d", "a/b/e/f", "a/g/h", "i/j").foldLeft(ResultT.ok[IO, Unit](())) {
      (result, path) => result >> store.utf8.write(path.toFilePath, "")
    } >> StoreDataUtil.listDir(store, "a".toFilePath)
  } must beOkLike(_.toSet ==== Set("a/b", "a/g").map(_.toFilePath))

  def listDirRoot = run { store =>
    store.utf8.write("a".toFilePath, "") >> StoreDataUtil.listDir(store, FilePath.root)
  } must beOkValue(List("/a".toFilePath))

  def listDirAbs = run { store =>
    store.utf8.write("a/b".toFilePath, "") >> StoreDataUtil.listDir(store, "/a".toFilePath)
  } must beOkValue(List("a/b".toFilePath))

  def listDirEmpty = run { store =>
    StoreDataUtil.listDir(store, "a".toFilePath)
  } must beOkValue(Nil)
}
