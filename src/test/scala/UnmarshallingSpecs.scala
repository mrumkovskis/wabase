package org.wabase

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshal}
import org.apache.pekko.util.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wabase.WabaseUnmarshallers.mapUnmarshaller

import scala.concurrent.{ExecutionContextExecutor, Future}

class UnmarshallingSpecs extends AnyFlatSpec with Matchers with ScalaFutures {
  implicit val system: ActorSystem = ActorSystem("wabase-test")
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  "FormDecoder.extractFormDataToMap" should "handle URL-encoded form data with multiple fields" in {
    val entity = HttpEntity(ContentTypes.`application/x-www-form-urlencoded`, "key1=value1&key2=value2")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map("key1" -> "value1", "key2" -> "value2"))
    }
  }

  it should "handle URL-encoded form data with special characters" in {
    val entity = HttpEntity(ContentTypes.`application/x-www-form-urlencoded`, "key1=value+with%20spaces&key2=value%26encoded")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map("key1" -> "value with spaces", "key2" -> "value&encoded"))
    }
  }

  it should "handle application/json content type with json object" in {
    val entity = HttpEntity(ContentTypes.`application/json`, """{"key": "value"}""")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map("key" -> "value"))
    }
  }

  it should "handle multipart form data with text fields" in {
    val formData = Multipart.FormData(
      Multipart.FormData.BodyPart("key1", HttpEntity("value1")),
      Multipart.FormData.BodyPart("key2", HttpEntity("value2"))
    )
    val entity = formData.toEntity("boundary")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map("key1" -> "value1", "key2" -> "value2"))
    }
  }

  it should "handle multipart form data with a file upload" in {
    val fileContent = "file content".getBytes("UTF-8")
    val formData = Multipart.FormData(
      Multipart.FormData.BodyPart("fileField", HttpEntity(ContentTypes.`text/plain(UTF-8)`, fileContent), Map("filename" -> "test.txt"))
    )
    val entity = formData.toEntity("boundary")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map(
        "fileField" -> Map("filename" -> "test.txt", "content_type" -> "text/plain; charset=UTF-8", "content" -> ByteString(fileContent)))
      )
    }
  }

  it should "handle multipart form data with both text and file fields" in {
    val fileContent = "file content".getBytes("UTF-8")
    val formData = Multipart.FormData(
      Multipart.FormData.BodyPart("textField", HttpEntity("value")),
      Multipart.FormData.BodyPart("fileField", HttpEntity(ContentTypes.`application/octet-stream`, fileContent), Map("filename" -> "test.txt"))
    )
    val entity = formData.toEntity("boundary")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map(
        "textField" -> "value",
        "fileField" -> Map("filename" -> "test.txt", "content_type" -> "application/octet-stream", "content" -> ByteString(fileContent))
      ))
    }
  }

  it should "fail for empty entity" in {
    val entity = HttpEntity.Empty
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future.failed) { ex =>
      ex shouldBe a[Exception]
      ex.getMessage should include("Unsupported content type")
    }
  }

  it should "fail for unsupported content types" in {
    val entity = HttpEntity(ContentTypes.`application/octet-stream`, ByteString("""{"key": "value"}"""))
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future.failed) { ex =>
      ex shouldBe a[Exception]
      ex.getMessage should include("Unsupported content type: application/octet-stream")
    }
  }

  it should "handle multiple values for the same key in URL-encoded form data" in {
    val entity = HttpEntity(ContentTypes.`application/x-www-form-urlencoded`, "key=value1&key=value2")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map("key" -> Vector("value1", "value2")))
    }
  }

  it should "handle multiple values for the same key in multipart form data" in {
    val fileContent = "file content".getBytes("UTF-8")
    val formData = Multipart.FormData(
      Multipart.FormData.BodyPart("textField", HttpEntity("value-1")),
      Multipart.FormData.BodyPart("fileField", HttpEntity(ContentTypes.`application/octet-stream`, fileContent), Map("filename" -> "test-1.txt")),
      Multipart.FormData.BodyPart("textField", HttpEntity("value-2")),
      Multipart.FormData.BodyPart("fileField", HttpEntity(ContentTypes.`application/octet-stream`, fileContent), Map("filename" -> "test-2.txt")),
    )
    val entity = formData.toEntity("boundary")
    val future = Unmarshal(entity).to[Map[String, Any]]
    whenReady(future) { result =>
      result should be(Map(
        "textField" -> Vector("value-1", "value-2"),
        "fileField" -> Vector(
          Map("filename" -> "test-1.txt", "content_type" -> "application/octet-stream", "content" -> ByteString(fileContent)),
          Map("filename" -> "test-2.txt", "content_type" -> "application/octet-stream", "content" -> ByteString(fileContent)),
        )
      ))
    }
  }
}
