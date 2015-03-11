package com.ambiata.ivory.cli.test

import com.ambiata.ivory.operation.ingestion.DictionaryImportValidate
import org.specs2.Specification

class genDictionarySpec extends Specification { def is = s2"""

  Self validate generated dictionary
    $validate
"""

  def validate =
    DictionaryImportValidate.validateSelf(dictionaryGen.dictionary).toEither ==== Right(())
}
