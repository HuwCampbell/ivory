package com.ambiata.ivory.operation.ingestion

import com.ambiata.ivory.core._
import com.ambiata.ivory.core.arbitraries._, ArbitraryEncodings._, ArbitraryDictionaries._, ArbitraryFeatures._
import org.specs2._
import scalaz.{Name => _,_}, Scalaz._

class DictionaryImportValidateSpec extends Specification with ScalaCheck { def is = s2"""

 A new dictionary
   is valid when the encoding is the same                          $encodingSame
   is invalid when the primitive encoding changes                  $primitiveEncodingChange
   is valid with a new struct                                      $newStruct
   validates when the struct changes                               $structChanges
   is valid normally                                               $selfValid
   is invalid with a hanging virtual definition                    $virtualMissingSource
   is invalid with a virtual definition with a virtual source      $virtualVirtualSource
   is invalid with a virtual definition with an invalid filter     $virtualInvalidFilter
   is invalid with a virtual definition with an invalid expression $virtualInvalidExpression

"""

  import DictionaryImportValidate._

  val fid = FeatureId(Name("a"), "b")
  val path = ValidationPath(fid)

  def encodingSame = prop((enc: Encoding) =>
    validate(dict(enc), dict(enc)) ==== OK
  )

  def primitiveEncodingChange = prop((e1: PrimitiveEncoding, e2: PrimitiveEncoding) => e1 != e2 ==> {
    validate(dict(e1), dict(e2)) ==== EncodingChanged(e1, e2, path).failureNel
  })

  def newStruct = prop((enc: StructEncoding) =>
    validate(Dictionary.empty, dict(enc)) ==== OK
  )

  def structChanges = prop((enc: PrimitiveEncoding) =>
    seqToResult(structChecks(enc, "x" :: path).map {
      case ((v1, v2), f) => validate(
        dict(StructEncoding(v1.map("x" ->).toMap)),
        dict(StructEncoding(v2.map("x" ->).toMap))
      ) ==== f.cata(_.failureNel, OK)
    })
  )

  // This is mainly about validating the arbitrary
  def selfValid = prop((dict: Dictionary) =>
    validateSelf(dict) ==== OK
  )

  def virtualMissingSource = prop((dict: VirtualDictionary, fid: FeatureId) => {
    val fullDict = Dictionary(List(Virtual(fid, dict.vd)))
    validateSelf(fullDict) ==== InvalidVirtualSource(dict.vd.source, ValidationPath(fid)).failureNel
  })

  def virtualVirtualSource = prop((vdict1: VirtualDictionary, vdict2: VirtualDictionary) => {
    val dict = vdict1.dictionary append Dictionary(List(Virtual(vdict2.fid, vdict2.vd.copy(source = vdict1.fid))))
    validateSelf(dict) ==== InvalidVirtualSource(vdict1.fid, ValidationPath(vdict2.fid)).failureNel
  })

  def virtualInvalidFilter = prop((vdict1: VirtualDictionary) => {
    val filter = FilterTextV0.asString(FilterStruct(FilterStructOp(FilterOpAnd, List("missing" -> FilterEquals(StringValue(""))), Nil)))
    // The actual validation of different bad filters is handled in FilterSpec
    val dict = vdict1.copy(
      cd = ConcreteDefinition(StructEncoding(Map()), Mode.State, None, "", Nil),
      vd = vdict1.vd.copy(query = vdict1.vd.query.copy(expression = Count, filter = Some(filter)))
    ).dictionary
    validateSelf(dict).toEither.left.map(_.head) must beLeft ((f: DictionaryValidateFailure) => f must beLike {
      case InvalidFilter(_, ValidationPath(p, Nil)) => p ==== vdict1.fid
    })
  })

  def virtualInvalidExpression = prop((vdict1: VirtualDictionary) => {
    // The actual validation of different bad filters is handled in ExpressionSpec
    val dict = vdict1.copy(vd = vdict1.vd.copy(query = vdict1.vd.query.copy(expression = SumBy("missing", "missing")))).dictionary
    validateSelf(dict).toEither.left.map(_.head) must beLeft((f: DictionaryValidateFailure) => f must beLike {
      case InvalidExpression(_, ValidationPath(p, Nil)) => p ==== vdict1.fid
    })
  })

  // At some point it might be worth investigating Prism's from Monocle to share this code with the actual logic
  private def structChecks(enc: PrimitiveEncoding, path: ValidationPath):
      List[((Option[StructEncodedValue], Option[StructEncodedValue]), Option[DictionaryValidateFailure])] = List(
    None                                   -> Some(StructEncodedValue(enc).opt)     -> None,
    None                                   -> Some(StructEncodedValue(enc))         -> Some(NotOptionalStructField(path)),
    Some(StructEncodedValue(enc).opt)      -> Some(StructEncodedValue(enc))         -> Some(NotOptionalStructField(path)),
    Some(StructEncodedValue(enc))          -> Some(StructEncodedValue(enc))         -> None,
    Some(StructEncodedValue(enc))          -> Some(StructEncodedValue(enc).opt)     -> None,
    Some(StructEncodedValue(enc))          -> None                                  -> Some(MissingStructField(path)),
    Some(StructEncodedValue(LongEncoding)) -> Some(StructEncodedValue(IntEncoding)) -> Some(EncodingChanged(LongEncoding, IntEncoding, path))
  )

  private def dict(enc: Encoding) =
    Dictionary(List(Definition.concrete(fid, enc, Mode.State, Some(BinaryType), "", Nil)))
}
