package com.ambiata.ivory.core

import scalaz._, Scalaz._

object Maps {

  def outerJoin[K, V1, V2](m1: Map[K, V1], m2: Map[K, V2]): Map[K, \&/[V1, V2]] =
    m1.map {
      case (k, v) => k -> m2.get(k).cata(\&/.Both(v, _), \&/.This[V1, V2](v))
    } ++ m2.flatMap {
      case (k, v) => m1.get(k).cata(_ => None, Some(k -> \&/.That[V1, V2](v)))
    }
}
