package com.ambiata.ivory.cli

import com.ambiata.ivory.core._

object ScoptReaders {
  implicit val CharRead: scopt.Read[Char] =
    scopt.Read.reads(str => {
      val chars = str.toCharArray
      chars.length match {
        case 0 => throw new IllegalArgumentException(s"'${str}' can not be empty!")
        case 1 => chars(0)
        case l => throw new IllegalArgumentException(s"'${str}' is not a char!")
      }
    })

  implicit val FactsetIdRead: scopt.Read[FactsetId] =
    scopt.Read.reads(s => FactsetId.parse(s) match {
      case None =>
        throw new IllegalArgumentException(s"'${s}' is not a valid factset-id.")
      case Some(s) =>
        s
    })

  implicit val SnapshotIdRead: scopt.Read[SnapshotId] =
    scopt.Read.reads(s => SnapshotId.parse(s) match {
      case None =>
        throw new IllegalArgumentException(s"'${s}' is not a valid snapshot-id.")
      case Some(s) =>
        s
    })

}
