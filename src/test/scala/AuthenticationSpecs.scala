package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class AuthenticationSpecs extends FlatSpec with Matchers {
  behavior of "Authentication"

  it should ("encode bytes to url safe base64 string and decode to same bytes") in {
    def decodeEncode(encoded: String, shouldTestReEncoded: Boolean = true) = {
      val decoded   = Authentication.Crypto.decodeBytes(encoded)
      val reEncoded = Authentication.Crypto.encodeBytes(decoded)
      val reDecoded = Authentication.Crypto.decodeBytes(reEncoded)
      decoded shouldBe reDecoded
      reEncoded
    }
    decodeEncode("Abc-def_3210")        shouldBe "Abc-def_3210"
    decodeEncode("Abc+def/3210")        shouldBe "Abc-def_3210"
    decodeEncode("Abc+def/3210++")      shouldBe "Abc-def_3210-w"
    decodeEncode("Abc-def_3210-w")      shouldBe "Abc-def_3210-w"
    decodeEncode("Abc+def/3210-w==")    shouldBe "Abc-def_3210-w"
    decodeEncode("Abc-def_3210-w==")    shouldBe "Abc-def_3210-w"
    decodeEncode("Abc+def/3210+++")     shouldBe "Abc-def_3210--8"
    decodeEncode("Abc\r\n  +def/3210")  shouldBe "Abc-def_3210"
  }
}
