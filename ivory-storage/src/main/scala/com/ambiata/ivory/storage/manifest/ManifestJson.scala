package com.ambiata.ivory.storage.manifest

import argonaut._, Argonaut._

object ManifestJson {
  def encoder[A](version: A => VersionManifest, tag: String, detail: A => Json): EncodeJson[A] =
    EncodeJson(a =>  Json(
      "version" := version(a)
    , "metadata" := ("type" := tag) ->: detail(a)
    ))

  def decoder[A](tag: String, decode: (VersionManifest, ACursor) => DecodeResult[A]): DecodeJson[A] =
    DecodeJson(c => for {
      versions <- c.get[VersionManifest]("version")
      m = c --\ "metadata"
      t <- m.get[String]("type")
      r <- decode(versions, m)
      _ <- if (t == tag) DecodeResult.ok(()) else DecodeResult.fail(s"The details are correct, but the type tag should be '$tag', but was '$t'.", c.history)
    } yield r)
}
