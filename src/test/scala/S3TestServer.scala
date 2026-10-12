package org.wabase

import com.typesafe.config.{Config, ConfigFactory}
import org.gaul.s3proxy.{AuthenticationType, S3Proxy}
import org.jclouds.ContextBuilder
import org.jclouds.blobstore.BlobStoreContext

import java.net.URI

/**
 * In-memory S3 server for tests (S3Proxy with jclouds transient blob store).
 *
 * S3Proxy does not support checksum headers (x-amz-checksum-*): by default request with such header is rejected
 * as not implemented, with ignoreUnknownHeaders header is accepted but checksum is neither validated nor returned.
 * Header is ignored here, so S3Storage checksum code is executed but S3 checksum validation is not tested.
 */
class S3TestServer {
  val accessKey = "access"
  val secretKey = "secret"

  private val blobStoreContext = ContextBuilder.newBuilder("transient")
    .credentials(accessKey, secretKey)
    .build(classOf[BlobStoreContext])
  private val s3Proxy = S3Proxy.builder()
    .blobStore(blobStoreContext.getBlobStore)
    .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, accessKey, secretKey)
    .endpoint(URI.create("http://127.0.0.1:0"))
    .ignoreUnknownHeaders(true)
    .build()

  def start(): this.type = {
    s3Proxy.start()
    while (s3Proxy.getState != "STARTED") Thread.sleep(10)
    this
  }

  def stop(): Unit = {
    s3Proxy.stop()
    blobStoreContext.close()
  }

  def createBucket(bucket: String): Unit =
    blobStoreContext.getBlobStore.createContainerInLocation(null, bucket)

  /** pekko-connectors-s3 settings for this server, to be used as `files.s3` of S3Storage */
  def s3Config: Config = ConfigFactory.parseString(
    s"""endpoint-url = "http://127.0.0.1:${s3Proxy.getPort}"
       |access-style = path
       |aws.credentials {
       |  provider = static
       |  access-key-id = "$accessKey"
       |  secret-access-key = "$secretKey"
       |}
       |aws.region {
       |  provider = static
       |  default-region = "us-east-1"
       |}""".stripMargin)

  /** File streamer `files` config for S3Storage on this server */
  def filesConfig(root: String, tmpPath: String, extra: String = ""): Config =
    ConfigFactory.parseString(
      s"""files.root = "$root"
         |files.tmp-path = "$tmpPath"
         |files.path-format = days
         |files.storage-class = org.wabase.fs.S3Storage
         |$extra""".stripMargin)
      .withFallback(s3Config.atPath("files.s3"))
}
