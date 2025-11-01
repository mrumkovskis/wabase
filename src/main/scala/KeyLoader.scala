package org.wabase

import com.typesafe.config.{Config, ConfigValueType}
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter

import java.io.{ByteArrayInputStream, StringReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security._
import java.util.Base64
import javax.crypto.SecretKey
import javax.crypto.spec.SecretKeySpec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * Provides methods to load cryptographic keys and certificates from a configuration.
 *
 * The `configPath` parameter specifies the location in the configuration where the key or certificate information is stored.
 * It can point to:
 *   - '''A string value''': Interpreted as a direct key/certificate string (e.g., PEM or DER encoded)
 *     or a file path if it has a known extension (e.g., `.cert`, `.crt`, `.der`, `.key`, `.p12`, `.pem`, `.pfx`).
 *   - '''A subconfig''': Must contain at least `path` or `value`. For PFX files, `password` is mandatory.
 *
 * Supported formats include PEM, DER, and PKCS12. File paths are recognized only by known extensions.
 * When a subconfig is used, optional fields such as `type`, `alias`, and `algorithm` can be specified:
 *   - `path`: File path to the key/certificate.
 *   - `value`: Direct string value of the key/certificate.
 *   - `password`: Required for PFX files or encrypted keys.
 *   - `type`: Optional format specifier (e.g., "PEM", "DER", "PKCS12", "BASE64", "HEX").
 *   - `alias`: Optional alias for PFX files.
 *   - `algorithm`: Optional algorithm for secret keys (defaults to "AES").
 */
object KeyLoader extends KeyLoader {
  private object Initializer {
    // Initialize Bouncy Castle provider
    val bcProvider = new org.bouncycastle.jce.provider.BouncyCastleProvider()
    Security.addProvider(bcProvider)
    def init: Unit = {}
  }
}

/**
 * Provides methods to load cryptographic keys and certificates from a configuration.
 *
 * The `configPath` parameter specifies the location in the configuration where the key or certificate information is stored.
 * It can point to:
 *   - '''A string value''': Interpreted as a direct key/certificate string (e.g., PEM or DER encoded)
 *     or a file path if it has a known extension (e.g., `.cert`, `.crt`, `.der`, `.key`, `.p12`, `.pem`, `.pfx`).
 *   - '''A subconfig''': Must contain at least `path` or `value`. For PFX files, `password` is mandatory.
 *
 * Supported formats include PEM, DER, and PKCS12. File paths are recognized only by known extensions.
 * When a subconfig is used, optional fields such as `type`, `alias`, and `algorithm` can be specified:
 *   - `path`: File path to the key/certificate.
 *   - `value`: Direct string value of the key/certificate.
 *   - `password`: Required for PFX files or encrypted keys.
 *   - `type`: Optional format specifier (e.g., "PEM", "DER", "PKCS12", "BASE64", "HEX").
 *   - `alias`: Optional alias for PFX files.
 *   - `algorithm`: Optional algorithm for secret keys (defaults to "AES").
 */
class KeyLoader extends Loggable {
  KeyLoader.Initializer.init
  /**
   * Detects the format and extracts the content based on the configuration value.
   * @param config The Typesafe Config object
   * @param configPath The path in the config where the key or certificate value or file path resides
   * @return A tuple of (format: String, content: String)
   * @throws java.lang.Exception if unable to detect format or read content
   */
  protected def getFormatAndContent(config: Config, configPath: String): (String, String) = {
    val hasSubConfig =
      config.hasPath(configPath) && config.getValue(configPath).valueType() == ConfigValueType.OBJECT
    if (hasSubConfig) {
      val subConfig   = config.getConfig(configPath)
      val isFile      = subConfig.hasPath("path")
      val hasPassword = subConfig.hasPath("password")
      val content =
        if (isFile)
          getFileContentAsString(subConfig.getString("path"))
        else
          subConfig.getString("value").replace("""\n""", "\n") // Restore newlines. Other control characters are not expected in this context.

      val format =
        if (subConfig.hasPath("type"))
          subConfig.getString("type").toUpperCase
        else if (isFile)
          detectFormatFromFile(subConfig.getString("path"), content)
        else if (hasPassword)
          "PKCS12" // JKS is deprecated, so PFX it is
        else
          detectFormatFromContent(content)

      // Return the tuple
      (format, content)
    } else {
      // Extract the value from the config
      val value = config.getString(configPath)

      // Determine if it’s a file path or direct content
      val isFile = looksLikeFilePath(value)

      // Extract the content
      val content = if (isFile) {
        getFileContentAsString(value)
      } else {
        value.replace("""\n""", "\n") // Restore newlines. Other control characters are not expected in this context.
      }

      // Detect the format
      val format = if (isFile) {
        detectFormatFromFile(value, content)
      } else {
        detectFormatFromContent(content)
      }

      // Return the tuple
      (format, content)
    }
  }

  private val knownExtensions = Set(".cert", ".crt", ".der", ".key", ".p12", ".pem", ".pfx")
  /** Helper method to determine if a string looks like a file path */
  protected def looksLikeFilePath(value: String): Boolean = {
    knownExtensions.exists(ext => value.toLowerCase.endsWith(ext))
  }

  private def getFileContentAsString(path: String) = {
    val bytes = Files.readAllBytes(Paths.get(path))
    if (isTextCertOrKey(bytes)) new String(bytes, StandardCharsets.UTF_8)
    // For DER or PKCS12, read as bytes and encode to base64 for consistency
    else Base64.getEncoder.encodeToString(bytes)
  }

  /** Detects format based on file extension and content */
  protected def detectFormatFromFile(filePath: String, content: String): String = {
    val extension = filePath.split('.').last.toLowerCase
    extension match {
      case "pem" | "crt" | "cert" =>
        if (content.startsWith("-----BEGIN")) "PEM"
        else throw new Exception("Invalid PEM file")
      case "p12" | "pfx" =>
        "PKCS12"
      case "der" =>
        "DER"
      case "key" =>
        if (content.startsWith("-----BEGIN")) "PEM"
        else if (isValidHexString(content)) "HEX"
        else if (isValidBase64(content)) "DER"
        else throw new Exception("Unknown format in .key file")
      case _ =>
        throw new Exception(s"Unsupported file extension: $extension")
    }
  }

  /** Detects format based on content */
  protected def detectFormatFromContent(content: String): String = {
    if (content.startsWith("-----BEGIN")) "PEM"
    else if (isValidHexString(content)) "HEX"
    else if (isValidBase64(content)) "DER"
    else throw new Exception("Unable to detect format from content")
  }

  /** Helper method to check if bytes represent readable text */
  protected def isTextCertOrKey(bytes: Array[Byte]): Boolean = {
    val printable = bytes.count(b => b >= 32 && b <= 126 || b == 10 || b == 13)
    printable == bytes.length
  }

  /** Helper method to validate if a string is valid Base64 */
  protected def isValidBase64(value: String): Boolean = {
    try {
      Base64.getDecoder.decode(value)
      true
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  protected def isValidHexString(str: String): Boolean =
    !str.isEmpty &&
      str.forall(c => (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))

  protected def loadFromKeystore[T <: AnyRef](content: String, format: String, config: Config, extract: (KeyStore, String, String) => T): T = {
    val bytes = Base64.getDecoder.decode(content)
    val ks = KeyStore.getInstance(format)
    val password =
      Option("password")
        .filter(config.hasPath)
        .map(config.getString)
        .getOrElse(throw new Exception(s"Password required for $format"))
    val aliasOpt =
      Option(s"alias")
        .filter(config.hasPath)
        .map(config.getString)
    ks.load(new ByteArrayInputStream(bytes), password.toCharArray)
    val aliases = ks.aliases()
    while (aliases.hasMoreElements) {
      val alias = aliases.nextElement()
      if (aliasOpt.isEmpty || aliasOpt.contains(alias))
        try {
          extract(ks, alias, password) match {
            case null =>
            case v: T @ unchecked => return v
          }
        } catch {
          case util.control.NonFatal(ex) =>
            logger.debug(s"Exception while processing keystore, ignoring alias $alias", ex)
        }
    }
    null.asInstanceOf[T]
  }

  /**
   * Loads a certificate from the configuration.
   *
   * The configuration at `configPath` can be:
   *   - '''A string''' Treated as a file path if it has a known extension (e.g., `.pem`, `.der`, `.pfx`), otherwise as a direct PEM or DER string.
   *   - '''A subconfig''': Must contain `path` or `value`; `password` is required for PFX.
   *
   * '''Supported subconfig fields''':
   *   - `path`: File path to the certificate.
   *   - `value`: Direct string value of the certificate.
   *   - `password`: Mandatory for PFX files.
   *   - `type`: Optional format specifier (e.g., "PEM", "DER", "PKCS12").
   *   - `alias`: Optional alias for PFX files.
   *
   * @param config the configuration object
   * @param configPath the path in the configuration where the certificate information is located
   * @return the loaded certificate
   * @throws java.lang.Exception if the certificate cannot be loaded or the configuration is invalid
   */
  def loadCertificate(config: Config, configPath: String): X509Certificate = {
    val (format, content) = getFormatAndContent(config, configPath)
    format match {
      case "PEM" =>
        val reader = new PEMParser(new StringReader(content))
        val obj = reader.readObject()
        obj match {
          case certHolder: X509CertificateHolder =>
            new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder)
          case _ => throw new Exception("Invalid PEM object for certificate")
        }
      case "DER" =>
        val bytes = Base64.getDecoder.decode(content)
        val certHolder = new X509CertificateHolder(bytes)
        new JcaX509CertificateConverter().setProvider("BC").getCertificate(certHolder)
      case "PKCS12" =>
        def getCertificate(ks: KeyStore, alias: String, password: String): X509Certificate = {
          ks.getCertificate(alias) match {
            case x509: X509Certificate => x509
            case _ => null
          }
        }
        val subConfig = config.getConfig(configPath) // subconfig for PKCS12 because of password
        loadFromKeystore(content, format, subConfig, getCertificate) match {
          case cert: X509Certificate => cert
          case null =>  throw new Exception("No X.509 certificate found in PKCS12 keystore")
        }
      case _ =>
        throw new Exception(s"Unsupported format: $format")
    }
  }

  /**
   * Loads a private key from the configuration.
   *
   * The configuration at `configPath` can be:
   *   - '''A string''' Treated as a file path if it has a known extension (e.g., `.pem`, `.der`, `.pfx`), otherwise as a direct PEM or DER string.
   *   - '''A subconfig''': Must contain `path` or `value`; `password` is required for PFX.
   *
   * '''Supported subconfig fields''':
   *   - `path`: File path to the private key.
   *   - `value`: Direct string value of the private key.
   *   - `password`: Mandatory for PFX files.
   *   - `type`: Optional format specifier (e.g., "PEM", "DER", "PKCS12").
   *   - `alias`: Optional alias for PFX files.
   *
   * @param config the configuration object
   * @param configPath the path in the configuration where the private key information is located
   * @return the loaded private key
   * @throws java.lang.Exception if the key cannot be loaded or the configuration is invalid
   */
   def loadPrivateKey(config: Config, configPath: String): PrivateKey = {
    val (format, content) = getFormatAndContent(config, configPath)
    format match {
      case "PEM" =>
        val reader = new PEMParser(new StringReader(content))
        val obj = reader.readObject()
        val converter = new JcaPEMKeyConverter().setProvider("BC")
        obj match {
          case pkInfo: PrivateKeyInfo => converter.getPrivateKey(pkInfo)
          case _ => throw new Exception("Invalid PEM object for private key")
        }
      case "DER" =>
        val bytes = Base64.getDecoder.decode(content)
        val pkInfo = PrivateKeyInfo.getInstance(bytes)
        val converter = new JcaPEMKeyConverter().setProvider("BC")
        converter.getPrivateKey(pkInfo)
      case "PKCS12" =>
        def getPrivateKey(ks: KeyStore, alias: String, password: String): PrivateKey =
          if (ks.isKeyEntry(alias)) {
            val key = ks.getKey(alias, password.toCharArray)
            key match {
              case privateKey: PrivateKey => privateKey
              case _ => null
            }
          } else null
        val subConfig = config.getConfig(configPath) // subconfig for PKCS12 because of password
        loadFromKeystore(content, format, subConfig, getPrivateKey) match {
          case pk: PrivateKey => pk
          case null =>  throw new Exception("No private key found in PKCS12 keystore")
        }
      case _ =>
        throw new Exception(s"Unsupported format: $format")
    }
  }

  /**
   * Loads a public key from the configuration.
   *
   * The configuration at `configPath` can be:
   *   - '''A string''' Treated as a file path if it has a known extension (e.g., `.pem`, `.der`, `.pfx`), otherwise as a direct PEM or DER string.
   *   - '''A subconfig''': Must contain `path` or `value`; `password` is required for PFX.
   *
   * '''Supported subconfig fields''':
   *   - `path`: File path to the public key or certificate.
   *   - `value`: Direct string value of the public key or certificate.
   *   - `password`: Mandatory for PFX files.
   *   - `type`: Optional format specifier (e.g., "PEM", "DER", "PKCS12").
   *   - `alias`: Optional alias for PFX files.
   *
   * If the input is a certificate, the public key will be extracted from it.
   *
   * @param config the configuration object
   * @param configPath the path in the configuration where the public key or certificate information is located
   * @return the loaded public key
   * @throws java.lang.Exception if the key cannot be loaded or the configuration is invalid
   */
  def loadPublicKey(config: Config, configPath: String): PublicKey = {
    val (format, content) = getFormatAndContent(config, configPath)
    format match {
      case "PEM" =>
        val reader = new PEMParser(new StringReader(content))
        val obj = reader.readObject()
        reader.close()
        obj match {
          case spki: SubjectPublicKeyInfo =>
            new JcaPEMKeyConverter().setProvider("BC").getPublicKey(spki)
          case keyPair: KeyPair =>
            keyPair.getPublic
          case certHolder: X509CertificateHolder =>
            new JcaX509CertificateConverter()
              .setProvider("BC")
              .getCertificate(certHolder)
              .getPublicKey
          case _ =>
            throw new Exception("Invalid PEM content: neither a public key nor a certificate")
        }
      case "DER" =>
        val bytes = Base64.getDecoder.decode(content)
        Try {
          val certFactory = CertificateFactory.getInstance("X.509", "BC")
          certFactory.generateCertificate(new ByteArrayInputStream(bytes)).asInstanceOf[X509Certificate]
        } match {
          case Success(certificate) =>
            certificate.getPublicKey
          case Failure(_) =>
            Try {
              SubjectPublicKeyInfo.getInstance(bytes)
            } match {
              case Success(spki) =>
                new JcaPEMKeyConverter().setProvider("BC").getPublicKey(spki)
              case Failure(ex) =>
                throw new Exception("Invalid DER content: neither a certificate nor a public key", ex)
            }
        }
      case "PKCS12" =>
        def getPublicKey(ks: KeyStore, alias: String, password: String): PublicKey = {
          val cert = ks.getCertificate(alias)
          if (cert != null) cert.getPublicKey else null
        }
        val subConfig = config.getConfig(configPath) // subconfig for PKCS12 because of password
        loadFromKeystore(content, format, subConfig, getPublicKey) match {
          case pk: PublicKey => pk
          case null =>  throw new Exception("No public key found in PKCS12 keystore")
        }
      case _ =>
        throw new Exception(s"Unsupported format: $format")
    }
  }

  /**
   * Loads a secret key from the configuration.
   *
   * The configuration at `configPath` can be:
   *   - '''A string''' Treated as a file path if it has a known extension (e.g., `.key`, `.p12`, `.pfx`), otherwise as a
   *     direct key string (e.g., BASE64, HEX).
   *   - '''A subconfig''': Must contain `value` or `path`, `password` for PFX files; `type` and `algorithm` are optional.
   *
   * '''Supported subconfig fields''':
   *   - `path`: File path to the secret key or keystore.
   *   - `value`: Direct string value of the secret key.
   *   - `password`: Mandatory for PFX files.
   *   - `type`: Optional format specifier (e.g., "BASE64", "HEX", "PKCS12").
   *   - `algorithm`: Optional algorithm for the secret key, defaults to "AES", used if format is not PKCS12.
   *   - `alias`: Optional alias for PFX files.
   *
   * @param config the configuration object
   * @param configPath the path in the configuration where the secret key information is located
   * @return the loaded secret key
   * @throws java.lang.Exception if the key cannot be loaded or the configuration is invalid
   */
  def loadSecretKey(config: Config, configPath: String): SecretKey = {
    val (format, content) = getFormatAndContent(config, configPath)
    lazy val hasSubConfig =
      config.hasPath(configPath) && config.getValue(configPath).valueType() == ConfigValueType.OBJECT
    lazy val subConfig = config.getConfig(configPath)
    lazy val algorithm =
      Option("algorithm").filter(_ => hasSubConfig).filter(subConfig.hasPath).map(subConfig.getString)
        .getOrElse("AES")
    format match {
      case "BASE64" | "DER" /* mis-detected Base64 */ =>
        val keyBytes = Base64.getDecoder.decode(content)
        new SecretKeySpec(keyBytes, algorithm)
      case "HEX" =>
        val keyBytes = hexStringToBytes(content)
        new SecretKeySpec(keyBytes, algorithm)
      case "PKCS12" =>
        def getSecretKey(ks: KeyStore, alias: String, password: String): SecretKey =
          if (ks.isKeyEntry(alias)) {
            val key = ks.getKey(alias, password.toCharArray)
            key match {
              case sk: SecretKey => sk
              case _ => null
            }
          } else null
        val subConfig = config.getConfig(configPath) // subconfig for PKCS12 because of password
        loadFromKeystore(content, format, subConfig, getSecretKey) match {
          case sk: SecretKey => sk
          case null =>  throw new Exception("No secret key found in PKCS12 keystore")
        }
      case _ => throw new Exception(s"Unsupported format: $format")
    }
  }

  /** Utility method to convert a hex string to a byte array. */
  private def hexStringToBytes(hex: String): Array[Byte] = {
    hex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
  }
}
