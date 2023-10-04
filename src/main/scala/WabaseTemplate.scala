package org.wabase

trait WabaseTemplate {
  def apply(template: String, data: Seq[Map[String, Any]]): TemplateResult
}

/**
 * Replaces all {{<key>}} in template with corresponding value from data.head
 * */
class SimpleTemplate extends WabaseTemplate {
  val templateRegex = """\{\{(\w+)\}\}""".r
  def apply(template: String, data: Seq[Map[String, Any]]): TemplateResult = {
    val dataMap = data.head
    val (idx, res) = templateRegex.findAllMatchIn(template).foldLeft(0 -> "") {
      case ((idx, r), m) =>
        (m.end, r + m.source.subSequence(idx, m.start) + String.valueOf(dataMap(m.group(1))))
    }
    StringTemplateResult(res + template.substring(idx))
  }
}
