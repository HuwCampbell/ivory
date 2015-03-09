package com.ambiata.ivory.mr

import com.ambiata.ivory.core.Crash

import scalaz._, Scalaz._

/**
 * <pre>
 *                  uuuuuuu
 *               uu$$$$$$$$$$$uu
 *           uu$$$$$$$$$$$$$$$$$uu
 *          u$$$$$$$$$$$$$$$$$$$$$u
 *         u$$$$$$$$$$$$$$$$$$$$$$$u
 *        u$$$$$$$$$$$$$$$$$$$$$$$$$u
 *        u$$$$$$$$$$$$$$$$$$$$$$$$$u
 *        u$$$$$$"   "$$$"   "$$$$$$u
 *        "$$$$"      u$u       $$$$"
 *         $$$u       u$u       u$$$
 *         $$$u      u$$$u      u$$$
 *          "$$$$uu$$$   $$$uu$$$$"
 *           "$$$$$$$"   "$$$$$$$"
 *             u$$$$$$$u$$$$$$$u
 *              u$"$"$"$"$"$"$u
 *   uuu        $$u$ $ $ $ $u$$       uuu
 *  u$$$$        $$$$$u$u$u$$$       u$$$$
 *   $$$$$uu      "$$$$$$$$$"     uu$$$$$$
 * u$$$$$$$$$$$uu    """""    uuuu$$$$$$$$$$
 * $$$$"""$$$$$$$$$$uuu   uu$$$$$$$$$"""$$$"
 *  """      ""$$$$$$$$$$$uu ""$"""
 *            uuuu ""$$$$$$$$$$uuu
 *   u$$$uuu$$$$$$$$$uu ""$$$$$$$$$$$uuu$$$
 *   $$$$$$$$$$""""           ""$$$$$$$$$$$"
 *    "$$$$$"                      ""$$$$""
 *      $$$"                         $$$$"
 * </pre>
 *
 * This is to represent an optional value in cases where we wish to avoid the cost of an extra allocation.
 * Please use with extreme care!
 */
class MutableOption[A](private var value: A, private var _isSet: Boolean) {

  def get: A = {
    if (!_isSet)
      Crash.error(Crash.DataIntegrity, "The value is currently unset, please check `set` first")
    value
  }

  def isSet: Boolean =
    _isSet

  def toOption: Option[A] =
    if (_isSet) Some(value) else None

  override def equals(a: Any): Boolean =
    if (a.isInstanceOf[MutableOption[_]]) {
      val that = a.asInstanceOf[MutableOption[A]]
      if (_isSet && that._isSet) value == that.value else _isSet == that._isSet
    } else false

  override def toString: String =
    s"MutableOption($value,$isSet)"
}

object MutableOption {

  def none[A](v: A): MutableOption[A] =
    new MutableOption[A](v, false)

  def some[A](v: A): MutableOption[A] =
    new MutableOption[A](v, true)

  def fromOption[A](o: Option[A], v: A): MutableOption[A] =
    o.cata(some, none(v))

  def set[A](o: MutableOption[A], value: A): MutableOption[A] = {
    o.value = value
    o._isSet = true
    o
  }

  def setNone[A](o: MutableOption[A]): MutableOption[A] = {
    o._isSet = false
    o
  }

  implicit def MutableOptionEqual[A: Equal]: Equal[MutableOption[A]] =
    Equal.equal {
      (a, b) => if(a._isSet && b.isSet) a.value === b.value else a._isSet == b._isSet
    }
}
