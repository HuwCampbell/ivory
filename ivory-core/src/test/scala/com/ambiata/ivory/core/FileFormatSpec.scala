package com.ambiata.ivory.core

import com.ambiata.ivory.core.ArgonautProperties._
import com.ambiata.ivory.core.arbitraries.Arbitraries._

import org.specs2._

import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._


class FileFormatSpec extends Specification with ScalaCheck { def is = s2"""

Laws
----

  Encode/Decode Json                           ${encodedecode[FileFormat]}
  Equal                                        ${equal.laws[FileFormat]}


Combinators
-----------

  FileFormat.Text fold only evalutes text expression:

     ${ prop((d: Delimiter, e: TextEscaping) =>
          FileFormat.text(d, e).fold((dd, ee) => (dd, ee), ???) ==== ((d, e))) }

  FileFormat.Thrift fold only evalutes thrift expression:

     ${ FileFormat.thrift.fold((_, _) => ???, ()) ==== (()) }

  Fold constructors is identity:

     ${ prop((o: FileFormat) =>
         o.fold((d, e) => FileFormat.text(d, e), FileFormat.thrift) ==== o) }

  fromString/render symmetry:

     ${ prop((o: FileFormat) => FileFormat.fromString(o.render) ==== o.some) }

   isText is true iff it is indeed text output format:

     ${ prop((f: Form, d: Delimiter, e: TextEscaping) => FileFormat.text(d, e).isText ==== true) }
     ${ FileFormat.thrift.isText ==== false }

   isThrift is true iff it is indeed thrift output format:

     ${ FileFormat.thrift.isThrift ==== true }
     ${ prop((d: Delimiter, e: TextEscaping) => FileFormat.text(d, e).isThrift ==== false) }

  isThrift/isText are exclusive:

     ${ prop((o: FileFormat) => o.isText ^ o.isThrift) }

Constructors
------------

   Lower-case constructors are just alias with the right type:

     ${ prop((d: Delimiter, e: TextEscaping) => FileFormat.text(d, e) ==== FileFormat.Text(d, e)) }
     ${ FileFormat.thrift ==== FileFormat.Thrift }

"""
}
