package com.ambiata.ivory.data

import DataSyntax._
import com.ambiata.mundane.io._
import com.ambiata.mundane.store._
import scalaz.{Store => _, _}, Scalaz._

object StoreDataUtil {

  // TODO Move to mudane
  def listDir[F[+_] : Functor](store: Store[F], path: FilePath): F[List[FilePath]] = {
    // relativeTo doesn't work as expected if path starts with /
    val path2 = if (path.path.startsWith("/")) path.path.substring(1).toFilePath else path
    store.list(path2).map(_.groupBy(_.relativeTo(path2).path.split("/").headOption).keys.flatten.map(path2 </> _).toList)
  }

  def listDirWithoutDotFiles[F[+_] : Functor](store: Store[F], path: FilePath): F[List[FilePath]] =
    listDir(store, path).map(_.filterHidden)
}
