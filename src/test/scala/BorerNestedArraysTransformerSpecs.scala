package org.wabase

import com.typesafe.config.ConfigFactory
import io.bullet.borer.{Cbor, Json, Target}
import io.bullet.borer.compat.pekko.{ByteStringOutput, ByteStringProvider}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers
import org.wabase.ResultEncoder.EncoderFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

class BorerNestedArraysTransformerSpecs extends FlatSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("borer-nested-arrays-transformer-specs")
  private implicit def ec: ExecutionContext = system.dispatcher

  override def afterAll(): Unit =
    Await.result(system.terminate(), 10.seconds)

  private val twoMebibytes = 2 * 1024 * 1024
  private lazy val xs = "x" * twoMebibytes
  private lazy val jsonDocument = ByteString(s"""{"subject":"s","body":"$xs"}""")
  private lazy val cborDocument = {
    val out = new ByteStringOutput
    val w = Cbor.writer(out)
    w.writeMapStart()
    w.writeString("subject")
    w.writeString("s")
    w.writeString("body")
    w.writeString(xs)
    w.writeBreak()
    out.result()
  }

  private def encoder(format: Target): EncoderFactory =
    os => BorerNestedArraysEncoder(os, format)

  /** Application-style override of the transformer limits, merged onto reference.conf. */
  private def raisedConfigs = {
    val merged = ConfigFactory.parseString(
      """
        |borer.transform.json.decoding.max-string-length = 2147483647
        |borer.transform.cbor.decoding.max-text-string-length = 2147483647
        |borer.transform.cbor.decoding.max-byte-string-length = 2147483647
      """.stripMargin
    ).withFallback(config)
    (
      BorerDecodeConfig.cbor(merged.getConfig("borer.transform.cbor.decoding")),
      BorerDecodeConfig.json(merged.getConfig("borer.transform.json.decoding")),
    )
  }

  private def runFlow(
    bytes: ByteString,
    format: Target,
    cborCfg: Cbor.DecodingConfig,
    jsonCfg: Json.DecodingConfig,
  ): ByteString =
    Await.result(
      Source.single(bytes)
        .via(BorerNestedArraysTransformer.flow(encoder(format), format, 1024, cborCfg, jsonCfg))
        .runWith(Sink.fold(ByteString.empty)(_ ++ _)),
      30.seconds,
    )

  private def runFlowDefault(bytes: ByteString, format: Target): ByteString =
    Await.result(
      Source.single(bytes)
        .via(BorerNestedArraysTransformer.flow(encoder(format), transformFrom = format))
        .runWith(Sink.fold(ByteString.empty)(_ ++ _)),
      30.seconds,
    )

  private def runTransform(
    bytes: ByteString,
    format: Target,
    cborCfg: Cbor.DecodingConfig,
    jsonCfg: Json.DecodingConfig,
  ): ByteString =
    BorerNestedArraysTransformer.transform(bytes, encoder(format), format, cborCfg, jsonCfg)

  private def runTransformDefault(bytes: ByteString, format: Target): ByteString =
    BorerNestedArraysTransformer.transform(bytes, encoder(format), format)

  private def assertFailure(snippet: String)(body: => Unit): Unit = {
    val caught = intercept[RuntimeException](body)
    val text = Iterator.iterate(caught: Throwable)(t => if (t == null) null else t.getCause)
      .takeWhile(_ != null)
      .flatMap(t => Option(t.getMessage))
      .mkString("\n")
    withClue(s"${caught.getClass.getName}: $text\n") {
      text should include (snippet)
    }
  }

  private def assertJsonBody(out: ByteString): Unit = {
    val text = out.utf8String
    val prefix = """{"subject":"s","body":""""
    val suffix = """"}"""
    text.startsWith(prefix) shouldBe true
    text.endsWith(suffix) shouldBe true
    val body = text.substring(prefix.length, text.length - suffix.length)
    body.length shouldBe xs.length
    body.forall(_ == 'x') shouldBe true
  }

  private def assertCborBody(out: ByteString): Unit =
    (out.indexOfSlice(ByteString(xs)) >= 0) shouldBe true

  behavior of "BorerNestedArraysTransformer decoding config"

  it should "default to borer.transform.*.decoding, substituted from request decoding" in {
    val json = BorerNestedArraysTransformer.defaultJsonDecodingConfig
    json.maxStringLength shouldBe config.getInt("borer.transform.json.decoding.max-string-length")
    json.maxStringLength shouldBe config.getInt("borer.json.decoding.max-string-length")
    json.maxStringLength shouldBe 1048576
    val cbor = BorerNestedArraysTransformer.defaultCborDecodingConfig
    cbor.maxTextStringLength shouldBe config.getInt("borer.transform.cbor.decoding.max-text-string-length")
    cbor.maxTextStringLength shouldBe config.getInt("borer.cbor.decoding.max-text-string-length")
    cbor.maxByteStringLength shouldBe config.getInt("borer.cbor.decoding.max-byte-string-length")
    val (raisedCbor, raisedJson) = raisedConfigs
    raisedJson.maxStringLength shouldBe 2147483647
    raisedJson.maxNumberMantissaDigits shouldBe json.maxNumberMantissaDigits
    raisedCbor.maxTextStringLength shouldBe 2147483647
    raisedCbor.maxByteStringLength shouldBe 2147483647
    raisedCbor.maxNestingLevels shouldBe cbor.maxNestingLevels
  }

  it should "reject a 2 MiB JSON string with the default limit and accept it when the limit is raised" in {
    val (cborCfg, jsonCfg) = raisedConfigs
    assertFailure("longer than configured maximum")(runFlowDefault(jsonDocument, Json))
    assertFailure("longer than configured maximum")(runTransformDefault(jsonDocument, Json))
    assertJsonBody(runFlow(jsonDocument, Json, cborCfg, jsonCfg))
    assertJsonBody(runTransform(jsonDocument, Json, cborCfg, jsonCfg))
  }

  // borer 1.7.2 through 1.18.0 keep maxTextStringLength and maxByteStringLength on
  // Cbor.DecodingConfig but never read them. A large CBOR text string therefore
  // round-trips under the default config. Array length is enforced, so that is what
  // shows the CBOR config is actually passed to the reader.
  it should "transform a 2 MiB CBOR text string and honor max-array-length" in {
    assertCborBody(runFlowDefault(cborDocument, Cbor))
    assertCborBody(runTransformDefault(cborDocument, Cbor))
    val arrayOfTwo = {
      val out = new ByteStringOutput
      val w = Cbor.writer(out)
      w.writeArrayHeader(2)
      w.writeInt(1)
      w.writeInt(2)
      out.result()
    }
    val low = BorerDecodeConfig.cbor(
      ConfigFactory.parseString("max-array-length = 1")
        .withFallback(config.getConfig("borer.transform.cbor.decoding"))
    )
    val jsonCfg = BorerNestedArraysTransformer.defaultJsonDecodingConfig
    assertFailure("greater than the configured maximum")(runFlow(arrayOfTwo, Cbor, low, jsonCfg))
    assertFailure("greater than the configured maximum")(runTransform(arrayOfTwo, Cbor, low, jsonCfg))
    runFlowDefault(arrayOfTwo, Cbor).nonEmpty shouldBe true
    runTransformDefault(arrayOfTwo, Cbor).nonEmpty shouldBe true
  }
}
