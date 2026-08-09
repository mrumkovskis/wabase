package org.wabase

import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.Base64

class PdfRendererSpecs extends FlatSpec with Matchers {

  /** Matches `app.template.assets.directories` in test application.conf (cwd = project root). */
  private val assetsDir: Path =
    Paths.get("src/test/resources/pdf-assets").toAbsolutePath.normalize

  private val logoFile: Path = assetsDir.resolve("logo.txt")

  private def readAll(in: InputStream): Array[Byte] =
    try in.readAllBytes() finally in.close()

  private def readUtf8(in: InputStream): String =
    new String(readAll(in), StandardCharsets.UTF_8).stripLineEnd

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // --- config from test application.conf ---

  "PdfRenderer assets config" should "load directories and classpath prefixes from test conf" in {
    PdfRenderer.assetsClasspathPrefixes should contain("/fonts/")
    PdfRenderer.assetsDirectories.map(_.normalize) should contain(assetsDir)
  }

  // --- data: (FontUtil / ImageUtil only) ---

  "PdfRenderer.openAsset" should "open data:font base64 payloads" in {
    val payload = "font-bytes"
    val uri = s"data:font/ttf;base64,${b64(payload)}"
    PdfRenderer.openAsset(uri).map(readUtf8) shouldBe Some(payload)
  }

  it should "open data:image base64 payloads" in {
    val payload = "image-bytes"
    val uri = s"data:image/png;base64,${b64(payload)}"
    PdfRenderer.openAsset(uri).map(readUtf8) shouldBe Some(payload)
  }

  it should "reject other data: schemes" in {
    PdfRenderer.openAsset(s"data:text/plain;base64,${b64("x")}") shouldBe None
    PdfRenderer.openAsset("data:application/octet-stream;base64,YQ==") shouldBe None
  }

  // --- classpath allowlist ---

  it should "open classpath assets under allowed prefixes" in {
    PdfRenderer.openAsset("/fonts/pdf-asset-test.txt").map(readUtf8) shouldBe Some("classpath-asset-ok")
  }

  it should "open classpath: URIs under allowed prefixes" in {
    PdfRenderer.openAsset("classpath:/fonts/pdf-asset-test.txt").map(readUtf8) shouldBe Some("classpath-asset-ok")
  }

  it should "reject classpath paths outside allowed prefixes" in {
    PdfRenderer.openAsset("/other/pdf-asset-test.txt") shouldBe None
    PdfRenderer.openAsset("classpath:/resource.txt") shouldBe None
  }

  // --- filesystem allowlist (test conf directories) ---

  it should "open files under configured directories (relative and file: URI)" in {
    Files.isRegularFile(logoFile) shouldBe true
    PdfRenderer.openAsset("logo.txt").map(readUtf8) shouldBe Some("disk-asset-ok")
    PdfRenderer.openAsset(logoFile.toUri.toString).map(readUtf8) shouldBe Some("disk-asset-ok")
  }

  it should "reject files outside configured directories" in {
    val other = Files.createTempDirectory("wabase-pdf-other")
    try {
      val outside = other.resolve("secret.txt")
      Files.writeString(outside, "secret")
      PdfRenderer.openAsset(outside.toUri.toString) shouldBe None
      PdfRenderer.openAsset(outside.toAbsolutePath.toString) shouldBe None
    } finally {
      Files.walk(other).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
    }
  }

  // --- traversal / encoding / SSRF ---

  it should "reject path traversal with .." in {
    PdfRenderer.openAsset("/fonts/../../../etc/passwd") shouldBe None
    PdfRenderer.openAsset("fonts/../fonts/pdf-asset-test.txt") shouldBe None
    PdfRenderer.openAsset("classpath:/fonts/foo/../../secret") shouldBe None
  }

  it should "reject encoded traversal and residual percent sequences" in {
    // %2e%2e -> ".." after one decode
    PdfRenderer.openAsset("/fonts/%2e%2e/%2e%2e/etc/passwd") shouldBe None
    // double-encoded: one decode leaves %
    PdfRenderer.openAsset("/fonts/%252e%252e/x") shouldBe None
    PdfRenderer.openAsset("/fonts/foo%2fbar") shouldBe None
  }

  it should "reject network and other disallowed schemes" in {
    PdfRenderer.openAsset("http://127.0.0.1/fonts/x") shouldBe None
    PdfRenderer.openAsset("https://example.com/fonts/x") shouldBe None
    PdfRenderer.openAsset("ftp://example.com/fonts/x") shouldBe None
    PdfRenderer.openAsset("http://evil/fonts/pdf-asset-test.txt") shouldBe None
  }

  it should "reject null and empty URIs" in {
    PdfRenderer.openAsset(null) shouldBe None
    PdfRenderer.openAsset("") shouldBe None
  }

  // --- end-to-end PDF render smoke ---

  "PdfRenderer.render" should "produce a non-empty PDF for simple XHTML" in {
    val html =
      """<!DOCTYPE html>
        |<html><head><title>t</title></head>
        |<body><p>Hello PDF</p></body></html>
        |""".stripMargin
    val baos = new ByteArrayOutputStream
    PdfRenderer.render(html, baos)
    val pdf = baos.toByteArray
    pdf.length should be > 100
    new String(pdf.take(5), StandardCharsets.ISO_8859_1) shouldBe "%PDF-"
  }

  it should "render when HTML references a blocked external image without opening it" in {
    val html =
      """<!DOCTYPE html>
        |<html><head><title>t</title></head>
        |<body>
        |  <p>With remote img</p>
        |  <img src="http://127.0.0.1:9/should-not-fetch.png"/>
        |</body></html>
        |""".stripMargin
    val baos = new ByteArrayOutputStream
    noException should be thrownBy PdfRenderer.render(html, baos)
    val pdf = baos.toByteArray
    pdf.length should be > 100
    new String(pdf.take(5), StandardCharsets.ISO_8859_1) shouldBe "%PDF-"
  }
}
