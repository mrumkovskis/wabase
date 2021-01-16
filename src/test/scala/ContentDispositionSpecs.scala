package org.wabase

import akka.http.scaladsl.model.headers.{ContentDispositionType, ContentDispositionTypes, RawHeader, `Content-Disposition`}
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

object ContentDispositionProducer extends BasicMarshalling {
  def params(filename: String): String =
    contentDisposition(filename, ContentDispositionTypes.attachment)
      .head.value.split(";", 2)(1).trim
}

class ContentDispositionSpecs extends FlatSpec with Matchers {

  import ContentDispositionProducer._

  it should "create Content-Disposition parameters for ascii filenames correctly" in {
    params("foo.txt")           should be("""filename="foo.txt"""")
    params("my file.txt")       should be("""filename="my file.txt"""")
    params("US-$ rates")        should be("""filename="US-$ rates"""")
    params(       "ALL: !#$&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~") should be(
      """filename="ALL: !#$&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~"""")
  }

  it should "handle some special ascii chars in filenames" in {
    params("esc-\"\\%.x")       should be("""filename="esc-???.x"; filename*=UTF-8''esc-%22%5C%25.x""")
    params("p=2*2+1€")          should be("""filename="p=2*2+1?"; filename*=UTF-8''p%3D2%2A2%2B1%E2%82%AC""")
  }

  it should "create Content-Disposition parameters for unicode filename correctly" in {
    params("foo-ä-€.html")      should be("""filename="foo-a-?.html"; filename*=UTF-8''foo-%C3%A4-%E2%82%AC.html""")
    params("£ and € rates")     should be("""filename="? and ? rates"; filename*=UTF-8''%C2%A3%20and%20%E2%82%AC%20rates""")
  }

  it should "encode some special ascii chars in unicode filenames correctly" in {
    params("foo-ä *.txt")       should be("""filename="foo-a *.txt"; filename*=UTF-8''foo-%C3%A4%20%2A.txt""")
  }

  it should "create Content-Disposition parameters for unicode filename nicely" in {
    params("naïve.txt")         should be("""filename="naive.txt"; filename*=UTF-8''na%C3%AFve.txt""")
    params("téléphone")         should be("""filename="telephone"; filename*=UTF-8''t%C3%A9l%C3%A9phone""")
    params("glāžšķūņu rūķīši")  should be("""filename="glazskunu rukisi"; filename*=UTF-8''gl%C4%81%C5%BE%C5%A1%C4%B7%C5%AB%C5%86u%20r%C5%AB%C4%B7%C4%AB%C5%A1i""")
  }
}
