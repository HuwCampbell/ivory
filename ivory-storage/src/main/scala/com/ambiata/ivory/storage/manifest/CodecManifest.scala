package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._
import scalaz._, Scalaz._

object EncodeManifest {
  def apply[A](tag: String, encode: A => (VersionManifest, Json)): EncodeJson[A] =
    EncodeJson(a => {
      val (version, detail) = encode(a)
      Json(
        "version" := version
      , "metadata" := ("type" := tag) ->: detail
      )
    })
}

object DecodeManifest {
  def apply[A](tag: String, decode: (VersionManifest, ACursor) => DecodeResult[A]): DecodeJson[A] =
    DecodeJson(c => for {
      versions <- c.get[VersionManifest]("version")
      m = c --\ "metadata"
      t <- m.get[String]("type")
      r <- decode(versions, m)
      _ <- (t == tag).unlessM(DecodeResult.fail(s"The details are correct, but the type tag should be '$tag', but was '$t'.", c.history))
    } yield r)
}

object CodecManifest {
  def apply[A](tag: String, encode: A => (VersionManifest, Json), decode: (VersionManifest, ACursor) => DecodeResult[A]): CodecJson[A] =
    CodecJson.derived(EncodeManifest(tag, encode), DecodeManifest(tag, decode))
}
