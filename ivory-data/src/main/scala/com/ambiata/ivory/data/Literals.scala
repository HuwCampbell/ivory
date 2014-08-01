package com.ambiata.ivory.data

import language.experimental.macros

object IvoryDataLiterals {
  implicit class IvoryDataLiteralContext(val c: StringContext) extends AnyVal {
    def k(): Key =
      macro IvoryDataLiteralsMacros.keyMacro

    def i(): Identifier =
      macro IvoryDataLiteralsMacros.identifierMacro

    def oi(): OldIdentifier =
      macro IvoryDataLiteralsMacros.oldIdentifierMacro
  }
}

object IvoryDataLiteralsMacros extends com.ambiata.ivory.reflect.MacrosCompat {

  def keyMacro(c: Context)(): c.Expr[Key] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_,List(Apply(_,List(Literal(Constant(str: String)))))) =>
        Key.create(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid Key[$str]"))
        c.Expr[Key](q"Key.create($str).get")
    }
  }

  def identifierMacro(c: Context)(): c.Expr[Identifier] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_,List(Apply(_,List(Literal(Constant(str: String)))))) =>
        Identifier.parse(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid Identifier[$str]"))
        c.Expr[Identifier](q"Identifier.parse($str).get")
    }
  }

  def oldIdentifierMacro(c: Context)(): c.Expr[OldIdentifier] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_,List(Apply(_,List(Literal(Constant(str: String)))))) =>
        OldIdentifier.parse(str).getOrElse(c.abort(c.enclosingPosition, s"Invalid OldIdentifier[$str]"))
        c.Expr[OldIdentifier](q"OldIdentifier.parse($str).get")
    }
  }
}
