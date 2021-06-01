package org.wabase

import java.util.Locale
import java.sql.SQLException
import java.lang.RuntimeException
import org.tresql.ChildSaveException
import org.yaml.snakeyaml.Yaml

import scala.language.postfixOps
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

trait DbConstraintMessage {
  def friendlyConstraintErrorMessage[T](f: => T)(implicit locale: Locale): T = friendlyConstraintErrorMessage(null, f)
  def friendlyConstraintErrorMessage[T](viewDef: AppMetadata#ViewDef , f: => T)(implicit locale: Locale): T
}

object DbConstraintMessage {
 trait NoCustomConstraintMessage extends DbConstraintMessage {
  override def friendlyConstraintErrorMessage[T](viewDef: AppMetadata#ViewDef , f: => T)(implicit locale: Locale): T = f
 }
 trait PostgreSqlConstraintMessage extends DbConstraintMessage with QuereaseProvider { this: I18n with Loggable  =>
  case class ConstraintViolationInfo(
    dbErrorCode: String,
    dbMessagePattern: String,
    genericNoDetailsMessage: String,
    genericMessage: String) {
    val namePattern = "[\\p{IsLatin}\\d\\.\\_]+"
    val nameExtractorPatternString =
      ("ERROR: " + dbMessagePattern)
        .replace("$$", s"""\"($namePattern)\"""")
        .replace("$", s"""\"$namePattern\"""")
        .replace(" ", "\\s+")
    val nameExtractor = nameExtractorPatternString.r
  }
  val Nn = ConstraintViolationInfo(
    "23502",
    "null value in column $$ violates not-null constraint",
    "Field must not be empty",
    """Field "%s" must not be empty""")
  val FkDel = ConstraintViolationInfo(
    "23503",
    "update or delete on table $ violates foreign key constraint $$ on table $",
    "Unable to find related entity",
    "Unable to find related entity (link %s)")
  val FkIns = ConstraintViolationInfo(
    "23503",
    "insert or update on table $ violates foreign key constraint $$",
    "Unable to find related entity",
    "Unable to find related entity (link %s)")
  val Uk = ConstraintViolationInfo(
    "23505",
    "duplicate key value violates unique constraint $$",
    "Value should be unique",
    "Value should be unique (constraint %s violated)")
  val Ck = ConstraintViolationInfo(
    "23514",
    "new row for relation $ violates check constraint $$",
    "Invalid data",
    "Invalid data (constraint %s violated)")
  /** Code "235BX" is recognized and handled here and therefore can be used
    * to raise custom business exceptions from custom db functions
    */
  val CustomDbBusinessException = ConstraintViolationInfo(
    "235BX",
    "(.*)",
    "Error 235BX",
    "%s")
  val constraintTranslationMap: Map[String, Any] = {
    val constraintYaml = "/constraint-translation.yaml"
    val resource = this.getClass.getResource(constraintYaml)
    if (resource == null) {
      logger.info(s"Default constraint error messages will be used - $constraintYaml not found in resources.")
      Map()
    } else {
      val source = scala.io.Source.fromURL(resource)
      Option(new Yaml().load(source.mkString)).map(_.asInstanceOf[java.util.Map[String, _]].asScala.toMap) getOrElse Map()
    }
  }

  def getConstraintTranslation(constraint: String, operation: String = "delete"): Option[String] = {
    val order = if (operation != "insert") 0 else 1
    val translations = constraintTranslationMap.get(constraint)
    if (translations != None) {
      translations.get match {
      case a: java.util.ArrayList[_] =>
        val sl = a.asScala.toList
        if (a.size < order + 1) None
        else Option((sl)(order).asInstanceOf[String])
      case _ => None
      }
    } else
      None
  }

  def postgreSqlConstraintGenericMessage(violation: ConstraintViolationInfo, details: String)(implicit locale: Locale): String = {
    translate(violation.genericMessage, details)
  }

  def getFriendlyConstraintErrorMessage(e: SQLException, viewDef: AppMetadata#ViewDef)(implicit locale: Locale): Nothing =
    getFriendlyConstraintErrorMessage(e, viewDef, Option(viewDef).map(_.table).orNull)

  def getFriendlyConstraintErrorMessage(e: SQLException, viewDef: AppMetadata#ViewDef, tableName: String)(implicit locale: Locale): Nothing = {
    val dbMsg = Option(e.getMessage) getOrElse ""

    val violation = e.getSQLState match {
      case Nn.dbErrorCode => Nn
      case FkDel.dbErrorCode => if (dbMsg.contains("update or delete")) FkDel else FkIns
      case Uk.dbErrorCode => Uk
      case Ck.dbErrorCode => Ck
      case CustomDbBusinessException.dbErrorCode => CustomDbBusinessException
      case _ => null
    }

    val name = // constraint name or column name
      Option(violation)
        .flatMap(_.nameExtractor.findFirstMatchIn(dbMsg))
        .map(_.group(1))
        .orNull
    if (name == null) throw e
    logger.info(dbMsg)
    val customMessage =
      if (violation == Nn){
        import AppMetadata._
        def viewLabel = for{
          vd <- Option(viewDef)
          if vd.table == tableName
          field <- vd.fields.find(f => f.name == name || f.saveTo == name)
          label = field.label
          if label != null
        } yield label

        def tableLabel = for{
          tableDef <- qe.tableMetadata.tableDefOption(tableName)
          column <- tableDef.cols.find(_.name == name)
          label = column.comments
          if label != null
        } yield label

        viewLabel.orElse(tableLabel).map(s => postgreSqlConstraintGenericMessage(violation, s))
      } else {
        if (violation == FkIns)
          getConstraintTranslation(name, "insert")
        else getConstraintTranslation(name)
      }

    val friendlyMessage =
      customMessage getOrElse postgreSqlConstraintGenericMessage(violation, name)
    throw new BusinessException(friendlyMessage, e)
  }
  override def friendlyConstraintErrorMessage[T](viewDef: AppMetadata#ViewDef, f: => T)(implicit locale: Locale): T = {
    def onSqlException(e: java.sql.SQLException, originalException: Throwable, tableName: String): T = {
      logger.debug(originalException.getMessage, originalException)
      Option(tableName)
        .map(t => getFriendlyConstraintErrorMessage(e, viewDef, t))
        .getOrElse(getFriendlyConstraintErrorMessage(e, viewDef))
    }

    def checkForRuntimeException(exception: Throwable, originalException: Throwable = null, tableName: String = null): T = exception match {
     case e: java.lang.RuntimeException => e.getCause match {
       case ee: java.sql.SQLException => onSqlException(ee, Option(originalException).getOrElse(exception), tableName)
       case _ => throw exception
     }
     case _ => throw exception
    }
    try f catch {
      case e: java.sql.SQLException => getFriendlyConstraintErrorMessage(e, viewDef)
      case e: org.tresql.ChildSaveException => e.getCause match {
        case ee: java.sql.SQLException => onSqlException(ee, e, e.tableName)
        case NonFatal(ee) => checkForRuntimeException(ee, e, e.tableName)
      }
      case NonFatal(e) => checkForRuntimeException(e)
    }
  }
 }
}
