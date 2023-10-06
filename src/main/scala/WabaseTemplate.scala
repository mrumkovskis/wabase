package org.wabase

import scala.concurrent.Future

trait WabaseTemplate {
  def apply(template: String, data: Seq[Map[String, Any]]): Future[TemplateResult]
}

/**
 * Replaces all {{<key>}} in template with corresponding value from data.head
 * */
class SimpleTemplate extends WabaseTemplate {
  val templateRegex = """\{\{(\w+)\}\}""".r
  def apply(template: String, data: Seq[Map[String, Any]]): Future[TemplateResult] = {
    val dataMap = data.head
    val (idx, res) = templateRegex.findAllMatchIn(template).foldLeft(0 -> "") {
      case ((idx, r), m) =>
        (m.end, r + m.source.subSequence(idx, m.start) + String.valueOf(dataMap(m.group(1))))
    }
    Future.successful(StringTemplateResult(res + template.substring(idx)))
  }
}
