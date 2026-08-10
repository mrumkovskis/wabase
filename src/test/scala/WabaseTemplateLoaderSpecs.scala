package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.{AnyFlatSpec => FlatSpec}
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

class WabaseTemplateLoaderSpecs extends FlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var system: ActorSystem = _
  private implicit def ec: ExecutionContext = system.dispatcher
  private implicit val fs: FileStreamer = null

  private val loader = new DefaultWabaseTemplateLoader

  private val templatesDir =
    Paths.get("src/test/resources/templates").toAbsolutePath.normalize

  override def beforeAll(): Unit = {
    system = ActorSystem("WabaseTemplateLoaderSpecs")
  }

  override def afterAll(): Unit = {
    if (system != null) Await.result(system.terminate(), 10.seconds)
  }

  private def load(name: String): Array[Byte] =
    Await.result(loader.load(name), 5.seconds)

  private def loadUtf8(name: String): String =
    new String(load(name), StandardCharsets.UTF_8).stripLineEnd

  "DefaultWabaseTemplateLoader config" should "load dir and classpath-prefix from test conf" in {
    loader.templateDirPath shouldBe Some(templatesDir)
    loader.classpathPrefix shouldBe Some("/templates/")
  }

  "DefaultWabaseTemplateLoader" should "load templates from configured directory" in {
    loadUtf8("hello.txt") shouldBe "Hello {{name}}!"
  }

  it should "reject filesystem path traversal" in {
    // falls through to raw string body when path is rejected
    intercept[BusinessException](loadUtf8("../../../../etc/passwd"))
      .getMessage shouldBe "Template not found: ../../../../etc/passwd"
  }

  it should "reject encoded traversal for filesystem templates" in {
    intercept[BusinessException](loadUtf8("%2e%2e/%2e%2e/etc/passwd"))
      .getMessage shouldBe "Template not found: %2e%2e/%2e%2e/etc/passwd"
  }

  it should "reject files outside template dir even with absolute path" in {
    val other = Files.createTempDirectory("wabase-tpl-other")
    try {
      val secret = other.resolve("secret.txt")
      Files.writeString(secret, "secret")
      intercept[BusinessException](loadUtf8(secret.toAbsolutePath.toString))
    } finally {
      Files.walk(other).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists(_))
    }
  }

  it should "load classpath templates under configured prefix" in {
    loadUtf8("/templates/hello.txt") shouldBe "Hello {{name}}!"
  }

  it should "reject classpath paths outside configured prefix" in {
    intercept[BusinessException](loadUtf8("/resource.txt"))
      .getMessage shouldBe "Template not found: /resource.txt"
  }

  it should "reject classpath path traversal" in {
    intercept[BusinessException](loadUtf8("/templates/../resource.txt"))
      .getMessage shouldBe "Template not found: /templates/../resource.txt"
  }

  it should "reject residual percent encoding on classpath paths" in {
    intercept[BusinessException](loadUtf8("/templates/%252e%252e/x"))
      .getMessage shouldBe "Template not found: /templates/%252e%252e/x"
  }

  it should "use template string as body when not found as path" in {
    intercept[BusinessException](loadUtf8("Hello inline {{x}}!"))
      .getMessage shouldBe "Template not found: Hello inline {{x}}!"
  }
}
