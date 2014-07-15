package com.ambiata.ivory.ingest

import com.ambiata.ivory.core._
import scalaz._, Scalaz._

object DictionaryImportValidate {

  type DictValidation[A] = ValidationNel[DictionaryValidateFailure, A]
  type DictValidationUnit = DictValidation[Unit]
  type StructName = String

  val OK: DictValidationUnit = Success(())

  def validate(oldDict: Dictionary, newDict: Dictionary): DictValidationUnit = {
    def validateEncoding(e1: Encoding, e2: Encoding, path: ValidationPath): DictValidationUnit = {
      (e1, e2) match {
        case (StructEncoding(sm1), StructEncoding(sm2)) =>
          sm1.toStream.foldMap[DictValidationUnit] {
            case (name, _) => sm2.get(name).cata(_ => OK, MissingStructField(name :: path).failureNel)
          } |+| sm2.toStream.foldMap {
            case (name, sv2) => sm1.get(name).cata({
              case sv1 if sv1 == sv2                   => OK
              case sv1 if sv1.encoding != sv2.encoding => EncodingChanged(sv1.encoding, sv2.encoding, name :: path).failureNel
              case _   if !sv2.optional                => NotOptionalStructField(name :: path).failureNel
              case sv1                                 => validateEncoding(sv1.encoding, sv2.encoding, name :: path)
            }, if (!sv2.optional)                         NotOptionalStructField(name :: path).failureNel else OK
            )
          }
        case _ if e1 != e2                             => EncodingChanged(e1, e2, path).failureNel
        case _ => OK
      }
    }
    def validateMeta(id: FeatureId, oldMeta: FeatureMeta, newMeta: FeatureMeta): DictValidationUnit = {
      validateEncoding(oldMeta.encoding, newMeta.encoding, ValidationPath(id))
    }
    oldDict.meta.toList.foldMap {
      case (fid, oldMeta) => newDict.meta.get(fid).map(validateMeta(fid, oldMeta, _)).getOrElse(OK)
    }
  }

  case class ValidationPath(id: FeatureId, path: List[StructName] = Nil) {
    def ::(name: StructName): ValidationPath = new ValidationPath(id, name :: path)

    override def toString: String =
      s"$id:${path.mkString("/")}"
  }

  sealed trait DictionaryValidateFailure {
    def toString: String
  }
  case class EncodingChanged(e1: Encoding, e2: Encoding, path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Encoding changed from $e1 to $e2 at $path"
  }
  case class MissingStructField(path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Struct field was removed $path"
  }
  case class NotOptionalStructField(path: ValidationPath) extends DictionaryValidateFailure {
    override def toString = s"Struct field $path was made optional"
  }
}
