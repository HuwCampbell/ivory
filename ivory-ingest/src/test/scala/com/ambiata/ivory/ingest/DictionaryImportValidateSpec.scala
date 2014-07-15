package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._, Arbitraries._
import org.specs2._
import scalaz._, Scalaz._

class DictionaryImportValidateSpec extends Specification with ScalaCheck { def is = s2"""

 A new dictionary
   is valid when the encoding is the same                          $encodingSame
   is invalid when the primitive encoding changes                  $primitiveEncodingChange
   is valid with a new struct                                      $newStruct
   validates when the struct changes                               $structChanges

"""

  import DictionaryImportValidate._

  val fid = FeatureId("a", "b")
  val path = ValidationPath(fid)

  def encodingSame = prop((enc: Encoding) =>
    validate(dict(enc), dict(enc)) ==== OK
  )

  def primitiveEncodingChange = prop((e1: PrimitiveEncoding, e2: PrimitiveEncoding) => e1 != e2 ==> {
    validate(dict(e1), dict(e2)) ==== EncodingChanged(e1, e2, path).failureNel
  })

  def newStruct = prop((enc: StructEncoding) =>
    validate(Dictionary(Map()), dict(enc)) ==== OK
  )

  def structChanges = prop((enc: PrimitiveEncoding) =>
    seqToResult(structChecks(enc, "x" :: path).map {
      case ((v1, v2), f) => validate(
        dict(StructEncoding(v1.map("x" ->).toMap)),
        dict(StructEncoding(v2.map("x" ->).toMap))
      ) ==== f.cata(_.failureNel, OK)
    })
  )

  // At some point it might be worth investigating Prism's from Monocle to share this code with the actual logic
  private def structChecks(enc: PrimitiveEncoding, path: ValidationPath):
      List[((Option[StructValue], Option[StructValue]), Option[DictionaryValidateFailure])] = List(
    None                            -> Some(StructValue(enc).opt)     -> None,
    None                            -> Some(StructValue(enc))         -> Some(NotOptionalStructField(path)),
    Some(StructValue(enc).opt)      -> Some(StructValue(enc))         -> Some(NotOptionalStructField(path)),
    Some(StructValue(enc))          -> Some(StructValue(enc))         -> None,
    Some(StructValue(enc))          -> Some(StructValue(enc).opt)     -> None,
    Some(StructValue(enc))          -> None                           -> Some(MissingStructField(path)),
    Some(StructValue(LongEncoding)) -> Some(StructValue(IntEncoding)) -> Some(EncodingChanged(LongEncoding, IntEncoding, path))
  )

  private def dict(enc: Encoding) =
    Dictionary(Map(fid -> FeatureMeta(enc, BinaryType, "")))
}
