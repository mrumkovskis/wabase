package org.wabase

import com.icegreen.greenmail.util.{GreenMail, ServerSetup}
import com.typesafe.config.{Config, ConfigFactory}
import jakarta.mail.internet.{ContentType, MimeMessage}
import jakarta.mail.Message.RecipientType
import jakarta.mail.{Multipart, Part}

import scala.jdk.CollectionConverters._

/** In-process smtp server for business scenario email tests. Mail is sent to it according to
  * 'simplejavamail' conf settings, received mail is examined through '/backdoor/inbox' and
  * cleared by '/backdoor/purge-inbox' (see [[BusinessScenariosBaseSpecs.backdoorAction]]). */
object GreenMailServer extends Loggable {
  private var server: GreenMail = null

  /** True if email is on and 'simplejavamail' smtp settings are provided to start server on. */
  def isEnabled(config: Config = ConfigFactory.load()): Boolean =
    Option("app.email.enabled").filter(config.hasPath).exists(config.getBoolean) &&
      config.hasPath("simplejavamail.smtp")

  /** Starts server on 'simplejavamail' smtp host and port if [[isEnabled]], does nothing if
    * already started. Returns true if server is running. Safe to call from any spec - projects
    * without email tests need not depend on greenmail, classes are loaded on first call only. */
  def startIfEnabled(config: Config = ConfigFactory.load()): Boolean = synchronized {
    if (server != null) true
    else if (!isEnabled(config)) false
    else {
      val host = config.getString("simplejavamail.smtp.host")
      val port = config.getInt   ("simplejavamail.smtp.port")
      val greenMail = new GreenMail(new ServerSetup(port, host, ServerSetup.PROTOCOL_SMTP))
      greenMail.start()
      server = greenMail
      logger.debug(s"Greenmail smtp server started on $host:$port")
      true
    }
  }

  /** Stops server if started, does nothing otherwise. */
  def stop(): Unit = synchronized {
    if (server != null) {
      try server.stop() finally server = null
      logger.debug("Greenmail smtp server stopped")
    }
  }

  def isRunning: Boolean = synchronized { server != null }

  private def running: GreenMail = synchronized {
    if (server == null)
      sys.error("Greenmail smtp server is not started, call GreenMailServer.startIfEnabled() first")
    server
  }

  /** Deletes all mail from all mailboxes. */
  def purgeInbox(): Unit = running.purgeEmailFromAllMailboxes()

  /** Received mail of all mailboxes as maps for comparison in scenario, ordered by mailbox and subject. */
  def inbox(): Seq[Map[String, Any]] = {
    val greenMail       = running
    val imapHostManager = greenMail.getManagers.getImapHostManager
    greenMail.getUserManager.listUser.asScala.toSeq.flatMap { user =>
      imapHostManager.getInbox(user).getMessages.asScala.toSeq.map { stored =>
        receivedMailToMap(user.getEmail, stored.getMimeMessage)
      }
    }.sortBy(mail => (String.valueOf(mail("mailbox")), String.valueOf(mail("subject"))))
  }

  /** Flattens (possibly nested) multipart message into leaf parts. */
  private def leafParts(part: Part): Seq[Part] = part.getContent match {
    case multipart: Multipart => (0 until multipart.getCount).flatMap(i => leafParts(multipart.getBodyPart(i)))
    case _ => Seq(part)
  }

  private def addresses(msg: MimeMessage, recipientType: RecipientType): String =
    Option(msg.getRecipients(recipientType)).map(_.map(_.toString).mkString(", ")).orNull

  /** Content-Disposition header as sent, folding whitespace collapsed. Exposed to verify
    * encoding of non-ascii attachment file names (rfc 2231) as seen by mail client. */
  private def contentDisposition(part: Part): String =
    Option(part.getHeader("Content-Disposition"))
      .map(_.mkString(" ").replaceAll("\\s+", " ").trim).orNull

  /** Received message as map for comparison in scenario. Attachment content is decoded as utf-8 -
    * email test attachments are text. Attachment content type is stripped of parameters (charset,
    * name) since these are added by mail library, not by wabase. */
  def receivedMailToMap(mailbox: String, msg: MimeMessage): Map[String, Any] = {
    val (attachments, bodies) = leafParts(msg).partition { part =>
      part.getFileName != null || Part.ATTACHMENT.equalsIgnoreCase(part.getDisposition)
    }
    Map(
      "mailbox"     -> mailbox,
      "from"        -> Option(msg.getFrom).map(_.map(_.toString).mkString(", ")).orNull,
      "to"          -> addresses(msg, RecipientType.TO),
      "cc"          -> addresses(msg, RecipientType.CC),
      "bcc"         -> addresses(msg, RecipientType.BCC),
      "reply_to"    -> Option(msg.getReplyTo).map(_.map(_.toString).mkString(", ")).orNull,
      "subject"     -> msg.getSubject,
      "body"        -> bodies.headOption.map(p => String.valueOf(p.getContent)).orNull,
      "body_content_type" -> bodies.headOption.map(p => new ContentType(p.getContentType).getBaseType).orNull,
      "attachments" -> attachments.map { part =>
        Map(
          "filename"     -> part.getFileName,
          "disposition"  -> contentDisposition(part),
          "content_id"   -> Option(part.getHeader("Content-ID")).map(_.mkString(" ")).orNull,
          "content_type" -> new ContentType(part.getContentType).getBaseType,
          "content"      -> new String(part.getInputStream.readAllBytes, "UTF-8"),
        )
      }.toList,
    )
  }
}
