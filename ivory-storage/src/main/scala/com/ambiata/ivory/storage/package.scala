package com.ambiata.ivory

import com.ambiata.mundane.store.{KeyName, Key}

package object storage {

  implicit class filterHiddenKeysSyntax(keys: List[Key]) {
    def filterHidden: List[Key] =
      keys.map(key => key.copy(components = key.components.filter(filterHiddenKeyName)))
        .filterNot(_.components.isEmpty)
  }

  def filterHiddenKeyName(keyName: KeyName): Boolean =
    !Seq("_", ".").exists(keyName.name.startsWith)

}
