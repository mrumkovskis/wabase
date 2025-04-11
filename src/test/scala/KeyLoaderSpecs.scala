package org.wabase

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import java.security.cert.X509Certificate
import java.security.{PrivateKey, PublicKey}
import java.util.Base64
import javax.crypto.SecretKey
import scala.io.Source

class KeyLoaderSpecs extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  behavior of "KeyLoader"

  // Sample key and certificate content (placeholders; replace with real data for actual testing)
  val derCertificateBase64 = Base64.getEncoder.encodeToString(getClass.getResourceAsStream("/sample-keys/sample-cert.der").readAllBytes())
  val derPrivateKeyBase64  = Base64.getEncoder.encodeToString(getClass.getResourceAsStream("/sample-keys/sample-private-key-unencrypted.der").readAllBytes())
  val derPublicKeyBase64   = Base64.getEncoder.encodeToString(getClass.getResourceAsStream("/sample-keys/sample-public-key.der").readAllBytes())
  val pemCertificate       = Source.fromResource("sample-keys/sample-cert.pem").mkString
  val pemPrivateKey        = Source.fromResource("sample-keys/sample-private-key-unencrypted.pem").mkString
  val pemPublicKey         = Source.fromResource("sample-keys/sample-public-key.pem").mkString
  val pfxCertAndKeyBase64  = Base64.getEncoder.encodeToString(getClass.getResourceAsStream("/sample-keys/sample-cert-and-private-key.pfx").readAllBytes())
  val pfxSecretKeyBase64   = Base64.getEncoder.encodeToString(getClass.getResourceAsStream("/sample-keys/sample-secret-key.pfx").readAllBytes())

  // Set up temporary files for file-based tests
  val tempDir = Files.createTempDirectory("keyloader-test").toString
  val pemPrivateKeyFile  = s"$tempDir/private.pem"
  val derPrivateKeyFile  = s"$tempDir/private.der"
  val pemPublicKeyFile   = s"$tempDir/public.pem"
  val derPublicKeyFile   = s"$tempDir/public.der"
  val pemCertificateFile = s"$tempDir/cert.pem"
  val derCertificateFile = s"$tempDir/cert.der"
  val pfxCertificateFile = s"$tempDir/cert.pfx"
  val pfxSecretKeyFile   = s"$tempDir/secretkey.pfx"

  // Write sample data to temporary files
  Files.write(Paths.get(pemPrivateKeyFile), pemPrivateKey.getBytes)
  Files.write(Paths.get(derPrivateKeyFile), Base64.getDecoder.decode(derPrivateKeyBase64))
  Files.write(Paths.get(pemPublicKeyFile), pemPublicKey.getBytes)
  Files.write(Paths.get(derPublicKeyFile), Base64.getDecoder.decode(derPublicKeyBase64))
  Files.write(Paths.get(pemCertificateFile), pemCertificate.getBytes)
  Files.write(Paths.get(pfxCertificateFile), Base64.getDecoder.decode(pfxCertAndKeyBase64))
  Files.write(Paths.get(pfxSecretKeyFile), Base64.getDecoder.decode(pfxSecretKeyBase64))
  Files.write(Paths.get(derCertificateFile), Base64.getDecoder.decode(derCertificateBase64))

  /** Cleanup: Delete temporary files */
  override def afterAll(): Unit = {
    Files.delete(Paths.get(pemPrivateKeyFile))
    Files.delete(Paths.get(derPrivateKeyFile))
    Files.delete(Paths.get(pemPublicKeyFile))
    Files.delete(Paths.get(derPublicKeyFile))
    Files.delete(Paths.get(pemCertificateFile))
    Files.delete(Paths.get(pfxCertificateFile))
    Files.delete(Paths.get(pfxSecretKeyFile))
    Files.delete(Paths.get(derCertificateFile))
    Files.delete(Paths.get(tempDir))
  }

  // Private Key Tests
  it should "load private key from PEM file" in {
    val config = ConfigFactory.parseString(s"""key = "$pemPrivateKeyFile"""")
    val privateKey = KeyLoader.loadPrivateKey(config, "key")
    privateKey shouldBe a[PrivateKey]
  }

  it should "load private key from DER file" in {
    val config = ConfigFactory.parseString(s"""key = "$derPrivateKeyFile"""")
    val privateKey = KeyLoader.loadPrivateKey(config, "key")
    privateKey shouldBe a[PrivateKey]
  }

  it should "load private key from PEM content" in {
    val pemContent = pemPrivateKey.replace("\n", """\\n""")
    val config = ConfigFactory.parseString(s"""key = "$pemContent"""")
    val privateKey = KeyLoader.loadPrivateKey(config, "key")
    privateKey shouldBe a[PrivateKey]
  }

  it should "load private key from DER content" in {
    val config = ConfigFactory.parseString(s"""key = "$derPrivateKeyBase64"""")
    val privateKey = KeyLoader.loadPrivateKey(config, "key")
    privateKey shouldBe a[PrivateKey]
  }

  it should "load private key from PFX file" in {
    val config = ConfigFactory.parseString(s"""certificate { path = "$pfxCertificateFile", password = wabase-test }""")
    val privateKey = KeyLoader.loadPrivateKey(config, "certificate")
    privateKey shouldBe a[PrivateKey]
  }

  // Public Key Tests
  it should "load public key from PEM file" in {
    val config = ConfigFactory.parseString(s"""key = "$pemPublicKeyFile"""")
    val publicKey = KeyLoader.loadPublicKey(config, "key")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from DER file" in {
    val config = ConfigFactory.parseString(s"""key = "$derPublicKeyFile"""")
    val publicKey = KeyLoader.loadPublicKey(config, "key")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from PEM content" in {
    val pemContent = pemPublicKey.replace("\n", """\\n""")
    val config = ConfigFactory.parseString(s"""key = "$pemContent"""")
    val publicKey = KeyLoader.loadPublicKey(config, "key")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from DER content" in {
    val config = ConfigFactory.parseString(s"""key = "$derPublicKeyBase64"""")
    val publicKey = KeyLoader.loadPublicKey(config, "key")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from PFX file" in {
    val config = ConfigFactory.parseString(s"""certificate { path = "$pfxCertificateFile", password = wabase-test }""")
    val publicKey = KeyLoader.loadPublicKey(config, "certificate")
    publicKey shouldBe a[PublicKey]
  }

  // Certificate Tests
  it should "load certificate from PEM file" in {
    val config = ConfigFactory.parseString(s"""certificate = "$pemCertificateFile"""")
    val certificate = KeyLoader.loadCertificate(config, "certificate")
    certificate shouldBe a[X509Certificate]
  }

  it should "load certificate from DER file" in {
    val config = ConfigFactory.parseString(s"""certificate = "$derCertificateFile"""")
    val certificate = KeyLoader.loadCertificate(config, "certificate")
    certificate shouldBe a[X509Certificate]
  }

  it should "load certificate from PEM content" in {
    val pemContent = pemCertificate.replace("\n", """\\n""")
    val config = ConfigFactory.parseString(s"""certificate = "$pemContent"""")
    val certificate = KeyLoader.loadCertificate(config, "certificate")
    certificate shouldBe a[X509Certificate]
  }

  it should "load certificate from DER content" in {
    val config = ConfigFactory.parseString(s"""certificate = "$derCertificateBase64"""")
    val certificate = KeyLoader.loadCertificate(config, "certificate")
    certificate shouldBe a[X509Certificate]
  }

  it should "load certificate from PFX file" in {
    val config = ConfigFactory.parseString(s"""certificate { path = "$pfxCertificateFile", password = wabase-test }""")
    val certificate = KeyLoader.loadCertificate(config, "certificate")
    certificate shouldBe a[X509Certificate]
  }

  // Public key from certificate Tests
  it should "load public key from certificate from PEM file" in {
    val config = ConfigFactory.parseString(s"""certificate = "$pemCertificateFile"""")
    val publicKey = KeyLoader.loadPublicKey(config, "certificate")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from certificate from DER file" in {
    val config = ConfigFactory.parseString(s"""certificate = "$derCertificateFile"""")
    val publicKey = KeyLoader.loadPublicKey(config, "certificate")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from certificate from PEM content" in {
    val pemContent = pemCertificate.replace("\n", """\\n""")
    val config = ConfigFactory.parseString(s"""certificate = "$pemContent"""")
    val publicKey = KeyLoader.loadPublicKey(config, "certificate")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from certificate from DER content" in {
    val config = ConfigFactory.parseString(s"""certificate = "$derCertificateBase64"""")
    val publicKey = KeyLoader.loadPublicKey(config, "certificate")
    publicKey shouldBe a[PublicKey]
  }

  it should "load public key from certificate from PFX file" in {
    val config = ConfigFactory.parseString(s"""certificate { path = "$pfxCertificateFile", password = wabase-test }""")
    val certificate = KeyLoader.loadPublicKey(config, "certificate")
    certificate shouldBe a[PublicKey]
  }

  // Secret Key Tests
  it should "load secret key from BASE64 content" in {
    val config = ConfigFactory.parseString("key = RCWvCCllCXjqo7h9E7TWJGsTzW4uLUc9JQ22ZxkfYH4")
    val secretKey = KeyLoader.loadSecretKey(config, "key")
    secretKey shouldBe a[SecretKey]
    secretKey.getAlgorithm should be("AES")
  }

  it should "load secret key from HEX content" in {
    val config = ConfigFactory.parseString("key = 4425af0829650978eaa3b87d13b4d6246b13cd6e2e2d473d250db667191f607e")
    val secretKey = KeyLoader.loadSecretKey(config, "key")
    secretKey shouldBe a[SecretKey]
    secretKey.getAlgorithm should be("AES")
  }

  it should "load secret key from PFX file" in {
    val config = ConfigFactory.parseString(s"""key { path = "$pfxSecretKeyFile", password = wabase-test }""")
    val secretKey = KeyLoader.loadSecretKey(config, "key")
    secretKey shouldBe a[SecretKey]
    secretKey.getAlgorithm should be("AES")
  }
}
