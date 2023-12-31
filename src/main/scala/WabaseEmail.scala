package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString

import jakarta.activation.DataSource
import org.simplejavamail.api.email.EmailPopulatingBuilder
import org.simplejavamail.api.mailer.Mailer
import org.simplejavamail.config.ConfigLoader
import org.simplejavamail.email.EmailBuilder
import org.simplejavamail.mailer.MailerBuilder

import java.io.{InputStream, OutputStream}
import java.util.Properties
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}


case class EmailAttachment(filename: String, content_type: String, content: Source[ByteString, _])

trait WabaseEmail {
  def sendMail(
    to: String,
    cc: String,
    bcc: String,
    from: String,
    replyTo: String,
    subject: String,
    body: String,
    attachments: Seq[EmailAttachment],
    async: Boolean /** Asynchronous sending flag */
  )(implicit
    ec: ExecutionContext,
    as: ActorSystem,
  ): Future[Unit]
}

class DefaultWabaseEmailSender extends WabaseEmail with Loggable {

  val isEnabled: Boolean = config.getBoolean("app.email.enabled")

  // extract simplejavamail properties from application conf to reduce configuration file count
  def simplejavamailPropertiesFromConfig: Properties = {
    import scala.jdk.CollectionConverters._
    val sjmConfig = config.getConfig("simplejavamail")
    val map: Map[String, String] = sjmConfig.entrySet.asScala.map({ entry =>
      s"simplejavamail.${entry.getKey}" -> Option(entry.getValue.unwrapped()).map(_.toString).orNull
    }).toMap
    val props = new Properties()
    map.foreach { case (k, v) => props.setProperty(k, v) }
    logger.debug("Simple Java Mail properties extracted from conf: " + props)
    props
  }

  if (isEnabled) ConfigLoader.loadProperties(simplejavamailPropertiesFromConfig, /*addProperties=*/true)
  val mailer: Mailer = if (isEnabled) MailerBuilder.buildMailer() else null

  override def sendMail(
    to: String,
    cc: String,
    bcc: String,
    from: String,
    replyTo: String,
    subject: String,
    body: String,
    attachments: Seq[EmailAttachment],
    async: Boolean /** Asynchronous sending flag */
  )(implicit
    ec: ExecutionContext,
    as: ActorSystem,
  ): Future[Unit] = {
    val builder = EmailBuilder.startingBlank
    def optional(setter: String => EmailPopulatingBuilder, value: String): Unit = {
      if (value != null && value != "")
        setter(value)
    }
    builder.to(to)
    optional(builder.cc _, cc)
    optional(builder.bcc _, bcc)
    optional(builder.from _, from)
    optional(builder.withReplyTo _, replyTo)
    builder.withSubject(subject)
    builder.withPlainText(body)
    attachments.foreach { attachment =>
      val filename = attachment.filename // org.apache.commons.lang3.StringUtils.stripAccents(attachment.filename)
      val inputStream: InputStream = attachment.content.runWith(StreamConverters.asInputStream())
      val dataSource = new DataSource {
        override def getContentType()   = attachment.content_type
        override def getInputStream()   = inputStream
        override def getName()          = filename
        override def getOutputStream()  = ???
      }
      builder.withAttachment(filename, dataSource)
    }
    val email = builder.buildEmail
    // // TODO (when scala 2.12 no longer supported):
    // Option(mailer).map(_.sendMail(email, async).asScala.map(_ => ())) getOrElse Future.successful(())
    Future {
      Option(mailer).foreach(_.sendMail(email, async))
    }
  }
}
