package com.ambiata.ivory.reflect

trait MacrosCompat {
  import language.experimental.macros
  type Context = scala.reflect.macros.Context
}

