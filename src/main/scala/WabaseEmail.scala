package org.wabase

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString

import com.typesafe.config.ConfigFactory
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
    subject: String,
    body: String,
    attachments: Seq[EmailAttachment] = Nil,
    cc: String = null,
    bcc: String = null,
    from: String = null,
    replyTo: String = null,
    html: Boolean = false, /** Send body as HTML? */
    async: Boolean = true, /** Asynchronous sending flag */
  )(implicit
    ec: ExecutionContext,
    as: ActorSystem,
  ): Future[Unit]
}

class DefaultWabaseEmailSender extends WabaseEmail with Loggable {

  val enabled: Boolean = config.getBoolean("app.email.enabled")

  // extract simplejavamail properties from application conf to reduce configuration file count
  def simplejavamailPropertiesFromConfig: Properties = {
    import scala.jdk.CollectionConverters._
    val sjmConfig =
      Some("simplejavamail").filter(config.hasPath).map(config.getConfig).getOrElse(ConfigFactory.empty)
    val map: Map[String, String] = sjmConfig.entrySet.asScala.map({ entry =>
      s"simplejavamail.${entry.getKey}" -> Option(entry.getValue.unwrapped()).map(_.toString).orNull
    }).toMap
    val props = new Properties()
    map.foreach { case (k, v) => props.setProperty(k, v) }
    logger.debug("Simple Java Mail properties extracted from conf: " + props)
    props
  }

  lazy val mailer: Mailer =
    if (enabled) {
      ConfigLoader.loadProperties(simplejavamailPropertiesFromConfig, /*addProperties=*/true)
      MailerBuilder.buildMailer()
    } else null

  override def sendMail(
    to: String,
    subject: String,
    body: String,
    attachments: Seq[EmailAttachment] = Nil,
    cc: String = null,
    bcc: String = null,
    from: String = null,
    replyTo: String = null,
    html: Boolean = false, /** Send body as HTML? */
    async: Boolean = true, /** Asynchronous sending flag */
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
    if (html)
      builder.withHTMLText(body)
    else
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
