package com.ambiata.ivory.scoobi

import org.specs2._
import org.scalacheck._, Arbitrary._
import com.ambiata.ivory.core.Arbitraries._
import java.io._
import com.ambiata.ivory.core._
import com.nicta.scoobi._, Scoobi._

import FactFormats._

class FactFormatsSpec extends Specification with ScalaCheck { def is = s2"""
  Can serialise/deserialise FactsetIds                 $factsetIds
  Can serialise/deserialise SnapshotIds                $snapshotIds
                                                       """
  def factsetIds = prop((id: FactsetId) => {
    val fmt = implicitly[WireFormat[FactsetId]]
    val bos = new ByteArrayOutputStream(2048)
    val out = new DataOutputStream(bos)
    fmt.toWire(id, out)
    out.flush()

    val actual = fmt.fromWire(new DataInputStream(new ByteArrayInputStream(bos.toByteArray)))
    actual must_== id
  })

  def snapshotIds = prop((id: SnapshotId) => {
    val fmt = implicitly[WireFormat[SnapshotId]]
    val bos = new ByteArrayOutputStream(2048)
    val out = new DataOutputStream(bos)
    fmt.toWire(id, out)
    out.flush()

    val actual = fmt.fromWire(new DataInputStream(new ByteArrayInputStream(bos.toByteArray)))
    actual must_== id
  })
}
