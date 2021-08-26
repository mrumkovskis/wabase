package org.wabase

import akka.util.ByteString

import java.io.{BufferedWriter, ByteArrayOutputStream, CharArrayWriter, OutputStreamWriter, Writer}

/** Support for reading and writing PosgreSQL Text Format files ready for COPY -
  * see https://www.postgresql.org/docs/current/sql-copy.html
  */
object PostgresTextFormat {
  val columnDelimiter = ByteString("\t")
  val rowDelimiter    = ByteString("\r\n")

  def writeString(s: String, out: Writer): Unit = {
    val len = s.length
    var pos = 0
    var isPlain = true
    while (isPlain && pos < len) {
      val ch = s.charAt(pos)
      isPlain = ch > '\r' && ch != '\\'
      pos += 1
    }
    if (isPlain)
      out write s
    else {
      pos -= 1
      if (pos > 0)
        out write s.substring(0, pos)
      while (pos < len) {
        val ch = s.charAt(pos)
        if (ch == '\\') out write "\\\\"
        else if (ch > '\r') out write ch
        else {
          val escaped = ch match {
            case '\u0000' => "\\000"
            case '\u0001' => "\\001"
            case '\u0002' => "\\002"
            case '\u0003' => "\\003"
            case '\u0004' => "\\004"
            case '\u0005' => "\\005"
            case '\u0006' => "\\006"
            case '\u0007' => "\\007"
            case '\b'     => "\\b"
            case '\t'     => "\\t"
            case '\n'     => "\\n"
            case '\u000b' => "\\v"
            case '\f'     => "\\f"
            case '\r'     => "\\r"
          }
          out write escaped
        }
        pos += 1
      }
    }
  }

  def write(value: Any, out: Writer): Unit = value match {
    case null =>
      out write """\N"""
    case true =>
      out write "true"
    case false =>
      out write "false"
    case x =>
      writeString(x.toString, out)
  }

  def writeColumns(columnNames: Seq[String], data: Map[String, Any], out: Writer): Unit = {
    var first = true
    columnNames foreach { columnName =>
      if (first) first = false else out write '\t'
      write(data.getOrElse(columnName, null), out)
    }
  }

  def writeRow(columnNames: Seq[String], data: Map[String, Any], out: Writer): Unit = {
    writeColumns(columnNames, data, out)
    out write "\r\n"
  }

  def encodeColumns(columnNames: Seq[String], data: Map[String, Any]): ByteString = {
    val baos = new ByteArrayOutputStream
    val out = new BufferedWriter(new OutputStreamWriter(baos, "UTF-8"))
    writeColumns(columnNames, data, out)
    out.close()
    ByteString(baos.toByteArray)
  }

  def decodeString(s: String): String = {
    val len = s.length
    var pos = 0
    var isPlain = true
    while (isPlain && pos < len) {
      val ch = s.charAt(pos)
      isPlain = ch != '\\'
      pos += 1
    }
    if (isPlain)
      s
    else if (s == "\\N")
      null
    else {
      pos -= 1
      val out = new CharArrayWriter(len)
      if (pos > 0)
        out write s.substring(0, pos)
      while (pos < len) {
        val ch = s.charAt(pos)
        if (ch == '\\') {
          pos += 1
          val ch2 = s.charAt(pos)
          val escaped = ch2 match {
            case '0' =>
              pos += 1
              val ch3 = s.charAt(pos)
              ch3 match {
                case '0' =>
                  pos += 1
                  val ch4 = s.charAt(pos)
                  ch4 match {
                    case '0' => '\u0000'
                    case '1' => '\u0001'
                    case '2' => '\u0002'
                    case '3' => '\u0003'
                    case '4' => '\u0004'
                    case '5' => '\u0005'
                    case '6' => '\u0006'
                    case '7' => '\u0007'
                  }
              }
            case '\\' => '\\'
            case 'b' => '\b'
            case 't' => '\t'
            case 'n' => '\n'
            case 'v' => '\u000b'
            case 'f' => '\f'
            case 'r' => '\r'
          }
          out write escaped
        } else out write ch
        pos += 1
      }
      out.close()
      out.toString
    }
  }
  def decodeColumns(columnNames: Seq[String], record: ByteString): Map[String, String] = {
    val s = record.utf8String
    val values = s.split('\t').map(decodeString).toList
    if (values.size != columnNames.size) {
      sys.error(s"Unexpected field count (${values.size} instead of ${columnNames.size}) for record: $s")
    }
    (columnNames zip values).toMap
  }
}
