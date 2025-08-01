package org.wabase

import java.util.Locale
import java.sql.SQLException
import java.lang.RuntimeException
import org.mojoz.metadata.{TableDef, ViewDef}
import org.tresql.ChildSaveException
import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.api.Load

import scala.language.postfixOps
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

trait DbConstraintMessage {
  def friendlyConstraintErrorMessage[T](f: => T)(implicit locale: Locale): T = friendlyConstraintErrorMessage(null, f)
  def friendlyConstraintErrorMessage[T](viewDef: ViewDef, f: => T)(implicit locale: Locale): T
}

object DbConstraintMessage {
 trait NoCustomConstraintMessage extends DbConstraintMessage {
  override def friendlyConstraintErrorMessage[T](viewDef: ViewDef, f: => T)(implicit locale: Locale): T = f
 }
 trait PostgreSqlConstraintMessageBuilder extends Loggable {
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
    "null value in column $$( of relation $)? violates not-null constraint",
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
      val loaderSettings = LoadSettings.builder()
        .setLabel("constraint translations ")
        .setAllowDuplicateKeys(false)
        .build();
      val source = scala.io.Source.fromURL(resource)
      Option(new Load(loaderSettings).loadFromString(source.mkString)).map {
        case m: java.util.Map[String @unchecked, _] => m.asScala.toMap
        case x => sys.error("Unexpected class: " + Option(x).map(_.getClass).orNull)
      }.getOrElse(Map())
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

  def postgreSqlConstraintGenericMessage(violation: ConstraintViolationInfo, details: String): String = {
    violation.genericMessage
  }

  def nameAndViolation(sqlCause: SQLException): (String, ConstraintViolationInfo) = {
   if (sqlCause == null) {
    (null, null)
   } else {
    val dbMsg = Option(sqlCause.getMessage) getOrElse ""

    val violation = sqlCause.getSQLState match {
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

    (name, violation)
   }
  }

  def friendlyMessageAndDetails(
    exception: Throwable, sqlCause: SQLException, viewDefOpt: Option[ViewDef], tableName: String,
    tableDefOption: (/*tableName:*/String, /*db:*/String) => Option[TableDef],
  ): (String, String) = {
    val (name, violation) = nameAndViolation(sqlCause) // constraint name or column name
    if (name == null) {
     (null, null)
    } else {
     Option(sqlCause.getMessage).foreach { dbMsg =>
      logger.info(dbMsg)
     }
     val (customMessage, details) = {
      if (violation == Nn){
        import AppMetadata._
        def viewLabel = for{
          vd <- viewDefOpt
          if vd.table == tableName
          field <- vd.fields.find(f => f.name == name || f.saveTo == name)
          label = field.label
          if label != null
        } yield label

        def tableLabel = for{
          tableDef <- tableDefOption(tableName, viewDefOpt.map(_.db).orNull)
          column <- tableDef.cols.find(_.name == name)
          label = column.comments
          if label != null
        } yield label

        viewLabel.orElse(tableLabel).map(s => (postgreSqlConstraintGenericMessage(violation, s), s))
      } else {
        if (violation == FkIns)
          getConstraintTranslation(name, "insert").map((_, name))
        else getConstraintTranslation(name).map((_, name))
      }
     }.map { case (msg, details) => (Some(msg), details) }
      .getOrElse(None, name)

     (customMessage getOrElse postgreSqlConstraintGenericMessage(violation, details), details)
    }
  }

  def getSqlCauseAndContext(e: Throwable): (SQLException, ChildSaveException) =
    getSqlCauseAndContext(e, e match { case ce: ChildSaveException => ce case _ => null})

  def getSqlCauseAndContext(e: Throwable, ce: ChildSaveException): (SQLException, ChildSaveException) = {
      e.getCause match {
        case ee: SQLException                   => (ee, ce)
        case ee: ChildSaveException if ee != e  => getSqlCauseAndContext(ee, ee)
        case ee: Throwable          if ee != e  => getSqlCauseAndContext(ee, ce)
        case _                                  => (null, ce)
      }
  }
 }

 object PostgreSqlConstraintMessageBuilder extends PostgreSqlConstraintMessageBuilder

 trait PostgreSqlConstraintMessage extends PostgreSqlConstraintMessageBuilder with DbConstraintMessage with QuereaseProvider { this: I18n =>

  def raiseFriendlyConstraintErrorMessage(
    exception: Throwable, sqlCause: SQLException, viewDef: ViewDef)(implicit locale: Locale): Nothing =
    raiseFriendlyConstraintErrorMessage(exception, sqlCause, viewDef, Option(viewDef).map(_.table).orNull)

  def raiseFriendlyConstraintErrorMessage(
    exception: Throwable, sqlCause: SQLException, viewDef: ViewDef, tableName: String)(implicit locale: Locale): Nothing =
    friendlyMessageAndDetails(exception, sqlCause, Option(viewDef), tableName, qe.tableMetadata.tableDefOption) match {
      case (null, _) =>
        throw exception
      case (friendlyMessage, details) =>
        val translated = translate(friendlyMessage, details)
        throw new BusinessException(translated, exception, details)
    }

  override def friendlyConstraintErrorMessage[T](viewDef: ViewDef, f: => T)(implicit locale: Locale): T = {
    try f catch {
      case e: SQLException => raiseFriendlyConstraintErrorMessage(e, e, viewDef)
      case NonFatal(e) => getSqlCauseAndContext(e) match {
        case (null, _) => throw e
        case (sqe, ce) =>
          Option(ce).map(_.name).filter(_ != null)
            .map(raiseFriendlyConstraintErrorMessage(e, sqe, viewDef, _))
            .getOrElse(raiseFriendlyConstraintErrorMessage(e, sqe, viewDef))
      }
    }
  }
 }
}
