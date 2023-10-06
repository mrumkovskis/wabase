package org.wabase

import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Future

case class EmailAttachment(filename: String, content_type: String, content: Source[ByteString, _])

trait WabaseEmail {
  def sendMail(
    to: String,
    cc: String,
    bcc: String,
    from: String,
    subject: String,
    body: String,
    attachments: Seq[EmailAttachment],
    async: Boolean /** Asynchronous sending flag */
  ): Future[Unit]
}
