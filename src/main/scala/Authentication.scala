package org.wabase

import java.security.SecureRandom
import java.util.Locale

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.directives.AuthenticationDirective
import akka.http.scaladsl.server.directives.SecurityDirectives
import akka.http.scaladsl.server.AuthenticationFailedRejection
import akka.http.scaladsl.server.AuthenticationFailedRejection.Cause
import akka.http.scaladsl.server.AuthenticationFailedRejection.CredentialsMissing
import akka.http.scaladsl.server.AuthenticationFailedRejection.CredentialsRejected

import scala.concurrent.Future
import scala.util.{Random, Try}
import Authentication.SessionInfoRemover
import Authentication.SessionUserExtractor

import scala.collection.immutable

trait Authentication[User] extends SecurityDirectives with SessionInfoRemover with SessionUserExtractor[User] { this: Execution =>

  import Authentication._

  /** String representation of user */
  def userInfo(implicit user: User): String = ???
  /** Encodes session. Encoded session will be encrypted and set as cookie {{{SessionCookieName}}} */
  def encodeSession(session: Session[User]): String
  /** Decodes session. Is used in {{{authenticate}}} directive which if successful provides
      user to inner route. */
  def decodeSession(session: String): Session[User]
  /** Signs in (logs in) user from http request. Can be implemented as Basic authentication, OAuth ... */
  def signInUser: AuthenticationDirective[User]

  /** Default implementation redirects to uri value stored in cookie {{{RequestedUriCookieName}}}
      or if cookie is missing redirects to / */
  def signInSuccessRoute(user: User) = (setSessionCookie(user) & optionalCookie(RequestedUriCookieName)) {
    _.map { requestedUriCookie =>
      deleteCookie(RequestedUriCookieName) {
        redirect(Uri(requestedUriCookie.value), StatusCodes.SeeOther)
      }
    }.getOrElse {
      redirect(Uri(SignedInDefaultPath), StatusCodes.SeeOther)
    }
  }
  /** Default implementation returns http Unauthorized with optional challenge */
  def signInFailedRoute(rejections: immutable.Seq[Rejection]): Route = {
    rejections.collectFirst {
      case AuthenticationFailedRejection(_, challenge) =>
        respondWithHeader(`WWW-Authenticate`(challenge)) {
          complete(StatusCodes.Unauthorized)
        }
    }
    .getOrElse(complete(StatusCodes.Unauthorized))
  }

  /** Signs out (logs out) user. Can do all necessary cleanup. Session cookie {{{SessionCookieName}}}
      is deleted by {{{signOut}}} directive. Default implementation does nothing. */
  def signOutUser(user: User): Future[akka.Done] = Future.successful(akka.Done)
  /** Default implementation redirects to / */
  def signOutRoute: Route = redirect(Uri(SignedOutPath), StatusCodes.SeeOther)
  /** Default implementation redirects to 'sign-in' uri */
  def authFailureRoute: Route = redirect(SignInPath, StatusCodes.SeeOther)
  /** Checks whether expiration time greater than current time and session ip and user agent matches with those of request */
  def validateSession(session: Session[User], ip: RemoteAddress, userAgent: Option[String]): Boolean =
    session.expirationTime > currentTime && session.ip == remoteAddressToString(ip) && session.userAgent == userAgent

  /** For session debugging, override to disable encryption but ensure session is cookie-compatible:
    * {{{ session.replace(",", "~").replace("\"", "'") }}}
    * In that case, 'decryption' would be:
    * {{{ session.replace("~", ",").replace("'", "\"") }}}
    */
  def encryptSession(session: String): String = Crypto.encrypt(session)
  def decryptSession(session: String): String = Crypto.decrypt(session)

  val SignInPath = "/sign-in"
  val SignedInDefaultPath = "/"
  val SignedOutPath = "/"
  val SessionCookieName = "session-id"
  val RequestedUriCookieName = "requested-uri"

  lazy val HttpChallengeRealm = "APP"
  lazy val AppDefaultChallenge = HttpChallenge("Any", HttpChallengeRealm)

  val sessionTimeOut = Try(scala.concurrent.duration.Duration(config.getString("session.timeout"))
    .toMillis).getOrElse(60L * 1000)

  val httpOnlyCookies = true
  val secureCookies = false

  def uniqueSessionId = new Random(new SecureRandom).alphanumeric.take(100).mkString

  def remoteAddressToString(a: RemoteAddress) = a.toIP.map(_.ip.toString).orNull
  protected val IP = "IP"
  protected val UserAgent = "User-Agent"

  def extractUserAgent = optionalHeaderValueByType(`User-Agent`).map(_.map(_.value))

  def extractSessionToken(user: User) = (extractClientIP.map(remoteAddressToString).filter(_ != null) & extractUserAgent)
    .recover { _ =>
       throw new BusinessException(
          "Client IP and/or User-Agent header(s) not found, ensure akka.http.server.remote-address-header = on")
    }.tmap { case (ip, userAgent) =>
      val expirationTime = currentTime + sessionTimeOut
      encryptSession(
        encodeSession(
          Session(
            user, ip, expirationTime, userAgent))
          ) -> expirationTime
    }

  def setSessionCookie(user: User): Directive0 = extractSessionToken(user)
    .tflatMap { case (sessionToken, _) =>
      setCookie(HttpCookie(
        SessionCookieName,
        value = sessionToken,
        path = Some("/"),
        httpOnly= httpOnlyCookies,
        secure = secureCookies
      ))
    }

  /** Extract session from session id cookie. If cookie does not exist throws rejection */
  def extractSession: Directive1[Option[Session[User]]] =
    cookie(SessionCookieName).map { cookie =>
      Try(decodeSession(decryptSession(cookie.value))).toOption
    }
  /** Extracts user from session, returns some user if session cookie is found and can be decoded,
    however, session is not validated */
  override def extractUserFromSession: Directive1[Option[User]] =
    extractSession.map(_.map(_.user)).recover(_ => provide(None))

  def signIn: Route = {
    val signinDirectiveWithFallback = signInUser.recover(r => StandardRoute(signInFailedRoute(r)): Directive1[User])
    signinDirectiveWithFallback{signInSuccessRoute}
  }

  /** Authenticates user from session cookie */
  def authenticateUser: AuthenticationDirective[Option[User]] =
    (extractSession & extractClientIP & extractUserAgent).tmap {
      case (session, ip, userAgent) => session
        .filter(validateSession(_, ip, userAgent))
        .map(_.user)
    }
  /** Authenticates user and provides authenticated user, updates session cookie. On failure executes {{{authFailureRoute}}} */
  def authenticate: AuthenticationDirective[User] = (handleRejections(authRejectionHandler) & authenticateUser)
    .flatMap {
      case Some(user) => setSessionCookie(user) & provide(user)
      case None => reject(AuthenticationFailedRejection(CredentialsRejected, AppDefaultChallenge)): Directive1[User]
    }

  def signOut: Route =
    (extractUserFromSession.flatMap {
      _.map { user => onSuccess(signOutUser(user)).flatMap (_ => pass) }.getOrElse(pass)
    } & removeSessionCookie)(signOutRoute)

  def removeSessionInfoFromRequest(req: HttpRequest) = {
    req.mapHeaders(_.flatMap {
      case c: Cookie =>
        c.cookies.filterNot(_.name == SessionCookieName) match {
          case s if s.isEmpty => Nil
          case s => List(new Cookie(s))
        }
      case h => List(h)
    })
  }

  /** Deletes session-id cookie if exists */
  protected def removeSessionCookie = deleteCookie(SessionCookieName, path = "/")
  /** On failed authentication sets requested-uri cookie if request has not been Ajax */
  protected def setRequestedUriCookie = (isAjaxRequest & deleteCookie(RequestedUriCookieName, path = "/")) |
    extractUri.flatMap { uri =>
      setCookie(HttpCookie(
        RequestedUriCookieName,
        value = uri.withScheme("").withAuthority(Uri.Authority.Empty).toString,
        path = Some("/"),
        httpOnly = true)
      )
  }
  def authRejectionHandler = RejectionHandler.newBuilder().handle {
    case MissingCookieRejection(SessionCookieName) =>
      setRequestedUriCookie { authFailureRoute }
    case AuthenticationFailedRejection(credentials, _) =>
      (removeSessionCookie & setRequestedUriCookie){ authFailureRoute }
  }.result()

  private val `X-Requested-With` = "X-Requested-With".toLowerCase
  val isAjaxRequest: Directive0 = headerValueByName(`X-Requested-With`).filter{ _ == "XMLHttpRequest"}.map(_ => ())
}

object Authentication {
  import com.lambdaworks.crypto.SCryptUtil
  import java.security.MessageDigest

  case class Session[User](user: User, ip: String, expirationTime: Long, userAgent: Option[String])

  /** Removes session info from request, used by [[org.wabase.DeferredControl]]
    * to calculate stable request hash */
  trait SessionInfoRemover {
    /** Removes session info from request, used by [[org.wabase.DeferredControl]]
      * to calculate stable request hash */
    def removeSessionInfoFromRequest(req: HttpRequest): HttpRequest
  }

  /** Extracts user from session, returns some user if session (cookie) is valid */
  trait SessionUserExtractor[User] {
    /** Extracts user from session, returns some user if session (cookie) is valid */
    def extractUserFromSession: Directive1[Option[User]]
  }

  def passwordHash(password: String) = {
    val (n, r, p) = (16384, 8, 1)
    SCryptUtil.scrypt(password, n, r, p)
  }
  def checkPassword(password: String, storedPasswordHash: String) = {
    def md5(s: String) =
      MessageDigest.getInstance("MD5").digest(s.getBytes)
        .map("%02X".format(_)).mkString.toLowerCase
    try {
      if (storedPasswordHash.length == 32) {
        if (md5(password) == storedPasswordHash.toLowerCase) {
          /*
          val saferPasswordHash = passwordHash(password)
          */
          // TODO update legacy md5 stored hash to scrypt!
          true
        } else false
      } else SCryptUtil.check(password, storedPasswordHash)
    } catch {
      case e: Exception =>
        throw new BusinessException("Unauthorized", e)
    }
  }

  object Crypto {

    import javax.crypto.{ Cipher, KeyGenerator, Mac }
    import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
    import System.currentTimeMillis
    import org.apache.commons.codec.binary.Base64
    import org.wabase.config

    val cryptoAlgoritm = "AES/CBC/PKCS5Padding"
    val macAlgoritm = "HmacSHA256"

    import java.security.SecureRandom
    private val randomGen = new ThreadLocal[SecureRandom] {
      override def initialValue = new SecureRandom
    }

    lazy val cryptoKey = secretKey("auth.crypto.key")
    lazy val macKey = secretKey("auth.mac.key")

    def secretKey(name: String) = Option(decodeBytes(config.getString(name)))
      .filter(_.length >= 16)
      //take whole number of power of 2 bytes, i.e. 16, 32, ...
      .map(a => a.take(Math.pow(2, (Math.log(a.length) / Math.log(2)).toInt).toInt))
      .getOrElse(sys.error("too short secret key, in base 64 encoded format must be at least 16 bytes long"))

    def randomBytes(size: Int) = {
      val array = Array.ofDim[Byte](size)
      randomGen.get.nextBytes(array)
      array ++ java.nio.ByteBuffer.allocate(8).putLong(currentTime).array.drop(2)
    }

    def encodeBytes(bytes: Array[Byte]) = Base64.encodeBase64URLSafeString(bytes)
    def decodeBytes(string: String) = Base64.decodeBase64(string)

    def encrypt(s: String) = {
      val rb = randomBytes(10)
      val encryptedSession = rb ++ code(s.getBytes("utf-8"),
          new IvParameterSpec(rb), Cipher.ENCRYPT_MODE)
      encodeBytes(hmac(encryptedSession) ++ encryptedSession)
    }
    def decrypt(s: String) = {
      val bytes = decodeBytes(s)
      val hmacBytes = bytes.take(32)
      val encryptedSession = bytes.drop(32)
      //check hmac
      if (hmac(encryptedSession).toSeq != hmacBytes.toSeq) sys.error("invalid HMAC")
      new String(code(encryptedSession.drop(16),
          new IvParameterSpec(encryptedSession.take(16)),
          Cipher.DECRYPT_MODE), "utf-8")
    }

    def code(s: Array[Byte], salt: IvParameterSpec, mode: Int) = {
      val ks = new SecretKeySpec(cryptoKey, "AES")
      val cipher = Cipher.getInstance(cryptoAlgoritm)
      cipher.init(mode, ks, salt)
      val bytes = cipher.doFinal(s)
      bytes
    }

    def hmac(s: Array[Byte]) = {
      val ks = new SecretKeySpec(macKey, "HMAC")
      val mac = Mac.getInstance(macAlgoritm)
      mac.init(ks)
      mac.doFinal(s)
    }

    def newKey = {
      //algrorithm name for key generator seems irrelevant
      val keyGen = KeyGenerator.getInstance("AES")
      keyGen.init(256) // for example
      val secretKey = keyGen.generateKey()
      encodeBytes(secretKey.getEncoded)
    }
  }

  trait BasicAuth[User] extends Authentication[User] { this: Execution =>

    def authenticateUser(username: String, password: String): Future[Option[User]]

    lazy val BasicChallenge = HttpChallenges.basic(HttpChallengeRealm)
    lazy val CustomChallenge = HttpChallenge("BasicOrOther", HttpChallengeRealm)

    override def signInUser = {
      def r(c: Cause, ch: HttpChallenge) = reject(AuthenticationFailedRejection(c, ch)): Directive1[User]
      extractCredentials.flatMap { _.collect {
          case BasicHttpCredentials(username, password) => onSuccess(authenticateUser(username, password))
        }.getOrElse(provide(Some(null).asInstanceOf[Option[User]])) //indicates missing credentials
      }.flatMap {
        case Some(user) if user != null => provide(user)
        case x =>
          val cause = x.map(_ => CredentialsMissing).getOrElse(CredentialsRejected)
          (isAjaxRequest & r(cause, CustomChallenge)) | r(cause, BasicChallenge)
      }
    }
  }

  trait LdapAuthentication { this: AppConfig with AppProvider[_] with Loggable =>

    import javax.naming._
    import javax.naming.directory.DirContext
    import javax.naming.directory.InitialDirContext
    import scala.util.Try

    val ldapUrl = Try(appConfig.getString("ldap-url")).toOption.getOrElse("")
    val accountPostfix = Try(appConfig.getString("account-postfix")).toOption.getOrElse("")

    def ldapLogin(username: String, password: String)(implicit locale: Locale): Unit = {
      val env = new java.util.Hashtable[String, String]()
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
      env.put(Context.PROVIDER_URL, ldapUrl)
      // Authenticate as S. User and password "mysecret"
      env.put(Context.SECURITY_AUTHENTICATION, "simple");

      if (username == null || "".equals(username)) {
        throw new BusinessException(app.translate("Wrong password or username"))
      }
      if (password == null || "".equals(password)) {
        throw new BusinessException(app.translate("Wrong password or username"))
      }
      env.put(Context.SECURITY_PRINCIPAL, username + accountPostfix)
      env.put(Context.SECURITY_CREDENTIALS, password)

      try {
        // Create the initial context
        val ctx = new InitialDirContext(env)
        ctx.close
      } catch {
        case e: AuthenticationException =>
          logger.error("Authentication failed", e)
          throw new BusinessException(app.translate("Authentication failed"))
        case e: Exception =>
          logger.error("Unexpected authentication error", e)
          throw new BusinessException(app.translate("Unexpected authentication error"))
      }
    }
  }
}
