package org.wabase

import akka.stream.{Attributes, Materializer, Outlet, SourceShape}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.util.{ByteString, ByteStringBuilder}
import org.wabase.Format.{xlsxDateTime, xsdDate}
import spray.json.JsonFormat

import java.io.{OutputStreamWriter, Writer}
import java.util.zip.ZipOutputStream
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.language.reflectiveCalls

trait RowWriter {
  def header(): Unit
  def hasNext: Boolean
  def row(): Unit
  def footer(): Unit
  def close(): Unit
}

trait RowWriters { this: QuereaseProvider =>

  class JsonRowWriter[T: JsonFormat](val result: Iterator[T], writer: Writer) extends RowWriter {
    import spray.json._
    override def header() = writer write "["

    var first = true
    override def row(): Unit = {
      if(first) first = false else writer write ",\n"
      writer write result.next().toJson.compactPrint
    }

    override def footer() = writer write "]\n"

    def hasNext = result.hasNext
    def close() = result match {
      case cr: AutoCloseable => cr.close()
      case _ =>
    }
  }

  trait AbstractRowWriter extends RowWriter {
    type Row <: {def values: Iterable[_]}
    type Result <: Iterator[Row] with AutoCloseable

    def labels: Seq[String]
    def row(r: Row): Unit
    def result: Result
    override def hasNext = result.hasNext
    override def row() = row(result.next())
    override def close() = result.close()
  }

  abstract class OdsRowWriter(zos: ZipOutputStream) extends AbstractRowWriter {

    import org.wabase.spreadsheet.ods._

    val streamer = new OdsStreamer(zos)

    override def header() = {
      streamer.startWorkbook
      streamer.startWorksheet
      streamer.startTable("dati")
      streamer.startRow
      labels foreach { h => streamer.cell(h) }
      streamer.endRow
    }
    override def row(r: Row) = {
      streamer.startRow
      r.values foreach { v: Any => streamer.cell(v) }
      streamer.endRow
    }
    override def footer() = {
      streamer.endTable
      streamer.endWorksheet
      streamer.endWorkbook
    }
  }

  abstract class CsvRowWriter(writer: Writer) extends AbstractRowWriter {
    def escapeValue(s: String) =
      if (s == null) null
      else if (s.contains(",") || s.contains("\"")) ("\"" + s.replaceAll("\"", "\"\"") + "\"")
      else s

    override def header() = {
      writer.write(labels.map(escapeValue).mkString("",",","\n"))
      writer.flush
    }
    override def row(r: Row) = {
      writer.write(r.values.map(csvValue).mkString("",",","\n"))
      writer.flush
    }
    override def footer() = {}

    def csvValue(v: Any): String = Option(v).map{
      case m: Map[String @unchecked, Any @unchecked] => ""
      case l: Traversable[Any] => ""
      case n: java.lang.Number => String.valueOf(n)
      case t: Timestamp => xlsxDateTime(t)
      case d: jDate => xsdDate(d)
      case x => x.toString
    }.map(escapeValue).getOrElse("")
  }

  abstract class XlsXmlRowWriter(writer: Writer) extends AbstractRowWriter {

    import org.wabase.spreadsheet.xlsxml._

    val headerStyle = Style("header", null, Font.BOLD)
    val streamer = new XlsXmlStreamer(writer)

    override def header() = {
      streamer.startWorkbook(Seq(headerStyle))
      streamer.startWorksheet("dati")
      streamer.startTable
      streamer.startRow
      labels foreach { h => streamer.cell(h, headerStyle) }
      streamer.endRow
    }
    override def row(r: Row) = {
      streamer.startRow
      r.values foreach { v: Any => streamer.cell(v) }
      streamer.endRow
    }
    override def footer() = {
      streamer.endTable
      streamer.endWorksheet
      streamer.endWorkbook
    }
  }
}

object RowSource {
  private class RowWriteSource (createRowWriter: Writer => RowWriter) extends GraphStage[SourceShape[ByteString]] {
    val out = Outlet[ByteString]("RowWriteSource")
    override val shape = SourceShape(out)
    override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {
      var buf: ByteStringBuilder = _
      var writer: OutputStreamWriter = _
      var src: RowWriter = _
      override def preStart() = {
        buf = new ByteStringBuilder
        writer = new OutputStreamWriter(buf.asOutputStream, "UTF-8")
        src = createRowWriter(writer)
        src.header()
        writer.flush()
      }
      override def postStop() = src.close()
      setHandler(out, new OutHandler {
        override def onPull = {
          while (buf.isEmpty && src.hasNext) {
            src.row()
            writer.flush()
          }
          if (!src.hasNext) {
            src.footer()
            writer.flush()
          }
          val chunk = buf.result()
          buf.clear()
          push(out, chunk)
          if (!src.hasNext) completeStage()
        }
      })
    }
  }

  private class RowWriteZipSource(createRowWriter: ZipOutputStream => RowWriter) extends GraphStage[SourceShape[ByteString]] {
    val out = Outlet[ByteString]("RowWriteSource")
    override val shape = SourceShape(out)
    override def createLogic(attrs: Attributes) = new GraphStageLogic(shape) {
      var buf: ByteStringBuilder = _
      var zos: ZipOutputStream = _
      var src: RowWriter = _
      override def preStart() = {
        buf = new ByteStringBuilder
        zos = new ZipOutputStream(buf.asOutputStream)
        src = createRowWriter(zos)
        src.header()
        zos.flush()
      }
      override def postStop() = src.close()
      setHandler(out, new OutHandler {
        override def onPull = {
          while (buf.isEmpty && src.hasNext) {
            src.row()
            zos.flush()
          }
          if (!src.hasNext) {
            src.footer()
            zos.close()
          }
          val chunk = buf.result()
          buf.clear()
          push(out, chunk)
          if (!src.hasNext) completeStage()
        }
      })
    }
  }
  import scala.language.implicitConversions
  /** Creates {{{RowWriteSource}}} and sets async boundary around. */
  implicit def createRowWriteSource(createRowWriter: Writer => RowWriter): Source[ByteString, _] =
    Source.fromGraph(new RowWriteSource(createRowWriter)).async

  /** Creates {{{RowWriteZipSource}}} and sets async boundary around. */
  implicit def createRowWriteZipSource(createRowWriter: ZipOutputStream => RowWriter): Source[ByteString, _] =
    Source.fromGraph(new RowWriteZipSource(createRowWriter)).async

  /** Runs {{{src}}} via {{{FileBufferedFlow}}} of {{{bufferSize}}} with {{{maxFileSize}}} to {{{CheckCompletedSink}}} */
  def value(bufferSize: Int,
            maxFileSize: Long,
            src: Source[ByteString, _],
            cleanupFun: Option[Throwable] => Unit = null)(implicit ec: ExecutionContext,
                                                          mat: Materializer): Future[SerializedResult] = {
    src
      .via(FileBufferedFlow.create(bufferSize, maxFileSize))
      .runWith(new ResultCompletionSink(cleanupFun))
  }
}
