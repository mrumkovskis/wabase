package org.wabase

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.util.ByteString
import pdi.jwt._
import pdi.jwt.algorithms.{JwtAsymmetricAlgorithm, JwtHmacAlgorithm}
import pdi.jwt.exceptions.{JwtLengthException, JwtNonNumberException, JwtNonStringException, JwtNonStringSetOrStringException, JwtValidationException}
import scala.util.{Failure, Success, Try}

import java.security.spec.X509EncodedKeySpec
import java.security.{KeyFactory, PublicKey}
import java.time.{Clock, Instant, ZoneId}
import java.util.Base64
import javax.crypto.SecretKey
import scala.collection.immutable.Map
import scala.jdk.CollectionConverters._

class JwtDecoder(config: Config) extends Loggable {
  // Claim mappings from short names to descriptive names
  private val claimMappings: Map[String, String] = {
    val mappingsConfig =
      if (config.hasPath("claim-mappings"))
        config.getConfig("claim-mappings")
      else ConfigFactory.empty
    mappingsConfig.entrySet.asScala.map { entry =>
      entry.getKey -> entry.getValue.unwrapped.toString
    }.toMap
  }

  // Clock configuration for time-based validations
  private val timeZone: ZoneId = ZoneId.of(config.getString("clock.time-zone"))
  private val clock: Clock = {
    if (config.hasPath("clock.fixed-instant")) {
      val instant = Instant.parse(config.getString("clock.fixed-instant"))
      Clock.fixed(instant, timeZone)
    } else {
      Clock.system(timeZone)
    }
  }
  implicit private val implicitClock: Clock = clock

  // JWT validation options
  private val jwtOptions: JwtOptions = {
    val optionsConfig = config.getConfig("options")
    JwtOptions(
      signature  = optionsConfig.getBoolean("validate-signature"),
      expiration = optionsConfig.getBoolean("validate-expiration"),
      notBefore  = optionsConfig.getBoolean("validate-not-before"),
      leeway     = optionsConfig.getLong("leeway")
    )
  }

  // Prepare sets of allowed HMAC and asymmetric algorithms in constructor
  private val allowedHmacAlgorithms: Seq[JwtHmacAlgorithm] =
    config.getStringList("allowed-algorithms").asScala
      .map(name => JwtAlgorithm.fromString(name))
      .collect { case a: JwtHmacAlgorithm => a }
      .toSeq
  private val allowedAsymmetricAlgorithms: Seq[JwtAsymmetricAlgorithm] =
    config.getStringList("allowed-algorithms").asScala
      .map(name => JwtAlgorithm.fromString(name))
      .collect { case a: JwtAsymmetricAlgorithm => a }
      .toSeq
  private val chooseKeyType =
    allowedHmacAlgorithms.nonEmpty && allowedAsymmetricAlgorithms.nonEmpty

  private val keyLoader = getObjectOrNewInstance[KeyLoader](config, "key-loader-class", "KeyLoader for JWT decoder")

  // Initialize secret key for HMAC or public key for asymmetric algorithms
  private val secretKeyOpt: Option[SecretKey] =
    Option("keys.secret-key").filter(config.hasPath).map(keyLoader.loadSecretKey(config, _))
  private val publicKeyOpt: Option[PublicKey] =
    Option("keys.public-key").filter(config.hasPath).map(keyLoader.loadPublicKey(config, _))

  // Optional issuer and audience for validation
  private val issuerOpt:   Option[String] = Option("accept.issuer"  ).filter(config.hasPath).map(config.getString)
  private val audienceOpt: Option[String] = Option("accept.audience").filter(config.hasPath).map(config.getString)

  private val jwtParser = JwtMap(clock)
  /**
   * Quick algorithm detection without full token decoding,
   * used only when both asymetric and hmac algorithms are allowed in config
   * @param token The JWT token string to decode
   * @return Option[JwtAlgorithm]
   */
  def extractAlgorithm(token: String): Option[JwtAlgorithm] = {
    val index = token.indexOf('.')
    if (index != -1) {
      val headerPart = token.substring(0, index)
      val headerJsonString = JwtBase64.decodeString(headerPart)
      jwtParser.extractAlgorithmOpt(headerJsonString)
    } else
      throw new JwtLengthException(
        s"Expected token [$token] to be composed of 2 or 3 parts separated by dots."
      )
  }

  private def keyTypeForToken(token: String): String = {
    if (chooseKeyType) {
      val algorithmOpt = extractAlgorithm(token)
      algorithmOpt match {
        case Some(a: JwtHmacAlgorithm) =>
          "hmac"
        case Some(a: JwtAsymmetricAlgorithm) =>
          "asymmetric"
        case _ =>
          null
      }
    } else if (allowedHmacAlgorithms.nonEmpty) {
      "hmac"
    } else {
      "asymmetric"
    }
  }

  /**
   * Decodes a JWT token and returns a map of claims.
   * @param token The JWT token string to decode
   * @return Map[String, Any] containing decoded claims, or empty map if decoding fails or token is invalid
   */
  def decodeToMap(token: String): Map[String, Any] = {
    Try {
      // Decode based on keyType, passing the appropriate set of algorithms
      keyTypeForToken(token) match {
        case "hmac" =>
          jwtParser.decodeJson(token, secretKeyOpt.get, allowedHmacAlgorithms, jwtOptions)
        case "asymmetric" =>
          jwtParser.decodeJson(token, publicKeyOpt.get, allowedAsymmetricAlgorithms, jwtOptions)
        case _ =>
          Failure(new JwtValidationException("Invalid algorithm"))
      }
    }.flatten match {
      case Success(claim) =>
        // Validate issuer and audience if specified
        val issValid =   issuerOpt.forall(iss => claim.get("iss").contains(iss))
        val audValid = audienceOpt.forall(aud => jwtParser.extractAudience(claim).exists(_.contains(aud)))
        if (issValid && audValid) {
          // Handle claims that can have multiple values (e.g., RFC 7519 audience, RFC 8693 scope)
          claim.map {
            case (k, v) if k == "aud" =>
              claimMappings.getOrElse(k, k) -> (v match {
                case s: String => Seq(s)            // Convert single string to Seq
                case seq: Seq[_] => seq             // Keep existing Seq
                case _ => Seq.empty                 // Handle invalid types
              })
            case (k, v) if k == "scope" =>
              claimMappings.getOrElse(k, k) -> (v match {
                case s: String => s.split("\\s+").filter(_ != "").toSeq // Convert single string to Seq (scopes are space separated)
                case seq: Seq[_] => seq             // Keep existing Seq
                case _ => Seq.empty                 // Handle invalid types
              })
            case (k, v) =>
              claimMappings.getOrElse(k, k) -> v
          }
        } else {
          Map.empty[String, Any]
        }
      case Failure(ex) =>
        logger.debug("Failed to decode or validate token", ex)
        Map.empty[String, Any]
    }
  }

  /**
   * Validates a JWT token - will throw exception if there are any errors. This method does not check issuer and audience.
   * @param token The JWT token string to validate
   */
  def validate(token: String): Unit = {
    // Decode based on keyType, passing the appropriate set of algorithms
    keyTypeForToken(token) match {
      case "hmac" =>
        jwtParser.validate(token, secretKeyOpt.get, allowedHmacAlgorithms, jwtOptions)
      case "asymmetric" =>
        jwtParser.validate(token, publicKeyOpt.get, allowedAsymmetricAlgorithms, jwtOptions)
      case _ =>
        throw new JwtValidationException("Invalid algorithm")
    }
  }
}

/** Implementation of `pdi.jwt.JwtCore` using Map[String, Any]
  */
trait JwtMapParser[H, C] extends JwtJsonCommon[Map[String, Any], H, C] {
  protected def parse(value: String): Map[String, Any] =
    CborOrJsonAnyValueDecoder.decodeToMap(ByteString(value))
  protected def stringify(value: Map[String, Any]): String =
    ResultEncoder.encodeAnyToJsonString(value)
  protected def getAlgorithm(header: Map[String, Any]): Option[JwtAlgorithm] =
    header.get("alg").flatMap {
      case  null     => None
      case "none"    => None
      case s: String => JwtAlgorithm.optionFromString(s)
      case _         => throw new JwtNonStringException("alg")
    }
  def extractAlgorithmOpt(header: String): Option[JwtAlgorithm] =
    parse(header).get("alg").flatMap {
      case "none" => None
      case s: String  => Option(JwtAlgorithm.fromString(s))
      case _ => None
    }
}

object JwtMap extends JwtMap(Clock.systemUTC) {
  def apply(clock: Clock): JwtMap = new JwtMap(clock)
}

class JwtMap(override val clock: Clock) extends JwtMapParser[JwtHeader, JwtClaim] {
  def parseHeader(header: String): JwtHeader = buildHeader(parse(header))

  def parseClaim(claim: String): JwtClaim = buildClaim(parse(claim))

  def extractAudience(claim: Map[String, Any]): Option[Set[String]] = extractSetOfS(claim, "aud")

  private def extractString(m: Map[String, Any], fieldName: String): Option[String] =
    m.get(fieldName).flatMap {
      case s: String  => Option(s)
      case null       => None
      case _          => throw new JwtNonStringException(fieldName)
    }

  private def extractSetOfS(m: Map[String, Any], fieldName: String): Option[Set[String]] =
    m.get(fieldName).map {
      case s: String   => Set(s)
      case seq: Seq[_] => seq.map {
        case s: String  => s
        case _          => throw new JwtNonStringSetOrStringException(fieldName)
      }.toSet
    }

  private def extractLong(m: Map[String, Any], fieldName: String): Option[Long] =
    m.get(fieldName).flatMap {
      case n: Number  => Option(n.longValue)
      case null       => None
      case _          => throw new JwtNonNumberException(fieldName)
    }

  private def buildHeader(header: Map[String, Any]): JwtHeader =
    JwtHeader(
      algorithm   = getAlgorithm(header),
      typ         = extractString(header, "typ"),
      contentType = extractString(header, "cty"),
      keyId       = extractString(header, "kid"),
    )

  private val builtInClaimKeys = Set("iss", "sub", "aud", "exp", "nbf", "iat", "jti")
  private def buildClaim(claim: Map[String, Any]): JwtClaim = {
    val contentMap = claim -- builtInClaimKeys
    JwtClaim(
      content     = ResultEncoder.encodeAnyToJsonString(contentMap),
      issuer      = extractString(claim, "iss"),
      subject     = extractString(claim, "sub"),
      audience    = extractSetOfS(claim, "aud"),
      expiration  = extractLong(claim,   "exp"),
      notBefore   = extractLong(claim,   "nbf"),
      issuedAt    = extractLong(claim,   "iat"),
      jwtId       = extractString(claim, "jti"),
    )
  }
}
