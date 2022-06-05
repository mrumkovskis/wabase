package org.wabase

import java.util.Date

import MapRecursiveExtensions._

import scala.language.implicitConversions
import scala.language.reflectiveCalls
import scala.util.{Try, Failure, Success}
import scala.util.control.NonFatal

import org.tresql._
import org.wabase.MapUtils._

/** Audit and all subimplementations use {{{qe.DTO}}} and {{{qe.DWI}}} */
trait Audit[User] { this: AppBase[User] =>
  def audit[C <: RequestContext[_]](originalContext: C)(action: => C): C
  def audit(context: AppActionContext, result: QuereaseResult): Unit
  def auditSave(id: jLong, viewName: String, instance: Map[String, Any], error: String)(implicit user: User, state: ApplicationState): Unit
  def auditLogin(user: User, loginInfo: qe.DTO): Unit
}

object Audit {
 trait NoAudit[User] extends Audit[User] { this: AppBase[User] =>
  def audit[C <: RequestContext[_]](originalContext: C)(action: => C): C = action
  def audit(context: AppActionContext, result: QuereaseResult): Unit = {}
  def auditSave(id: jLong, viewName: String, instance: Map[String, Any], error: String)(implicit user: User, state: ApplicationState): Unit = {}
  def auditLogin(user: User, loginInfo: qe.DTO): Unit = {}
 }


 trait AbstractAudit[User] extends Audit[User]{this: AppBase[User] =>
   import qe.{viewDefOption, tableMetadata}
   private def now = new Date()

   case class AuditData(
     action: String = null,
     entity_id: jLong = null,
     user: User,
     time: jDate = null,
     entity: String = null,
     newData: Map[String, Any] = null,
     oldData: Map[String, Any] = null,
     diff: List[Map[String, Any]] = null,
     relevant_id: List[Long] = null,
     error: String = null
   )

   def audit(data: AuditData): Unit

   override def audit[C <: RequestContext[_]](originalContext: C)(action: => C) = {
     val (res, error) = try {
       (action, null)
     } catch {
       case NonFatal(e) =>
         if (e.isInstanceOf[BusinessException])
           logger.info(e.getMessage, e)
         else
           logger.error(e.getMessage, e)
         (originalContext, e)
     }
     audit(res, error)
     if (error != null)
       throw error
     res
   }

   def logUnchangedSaves = false
   val blackListedFields = Set("auth", "password", "repeated_password", "passwd")
   def removeBlacklistedFields(m: Map[String, Any]) = m.map{
     case (k, v) if blackListedFields(k) & v != null => k -> "********"
     case r => r
   }

   def relevantKeys(view: String) = Set("id")

   def createRelevantIdExtractor: PartialFunction[Any, Long] = {
     case (_, id: Long) => id
   }
   lazy val relevantIdExtractor = createRelevantIdExtractor

   def getRelevantIds(viewName: String, viewId: Long, data: Map[String, Any], loadFromDb: Boolean, error: Throwable) = {
     if(error!= null){
       try dbAccess.tresqlResources.conn.rollback catch {
         case e: Exception => logger.error(e.getMessage, e)
       }
     }
     val fieldsInPojo = Option(data).map(_.filter(kv => relevantKeys(viewName).contains(kv._1)).filter(_._2 != null).toMap).getOrElse(Map.empty)

     val missingFields =
      Option(viewName)
        .filter(_ => loadFromDb)
        .flatMap(viewDefOption)
        .flatMap(v => Option(v.table).map(v.db -> _))
        .flatMap{ case (db, tableName) =>
       val tableCols = tableMetadata.tableDef(tableName, db).cols
       val relevantFieldsFromTable = tableCols.map(_.name).filter(relevantKeys(viewName)).filterNot(fieldsInPojo.contains)
       if (relevantFieldsFromTable.nonEmpty)
         Query(s"${Option(db).map(db => s"db:").getOrElse("")}$tableName[?]{${relevantFieldsFromTable.mkString(",")}}", viewId)(dbAccess.tresqlResources)
           .toListOfMaps.headOption.map(_.filter(_._2 != null))
       else None
     }

     (fieldsInPojo ++ missingFields.getOrElse(Map.empty) + ("id" -> viewId)).map {
       relevantIdExtractor
     }.toList
   }

   private def mapFromObj(obj: qe.DTO) = Option(obj).map(qe.toMap).map(removeBlacklistedFields).map(_.recursiveMap{
     case (_, m: Map[String, Any] @unchecked) => removeBlacklistedFields(m)
   }) getOrElse Map.empty

   def keyFields: List[String] = Nil
   def getDiff(oldObj: Map[String, Any], newObj: Map[String, Any]) = jsonizeDiff(diffMaps(oldObj, newObj, keyFields))

   override def auditLogin(user: User, loginInfo: qe.DTO) = {
     audit(AuditData(action = "login", user = user, newData = mapFromObj(loginInfo), time = now))
   }

  def logView(id: jLong, view: String, user: User, inParams: Map[String, Any], state: Map[String, Any], data: Map[String, Any]): Unit = {
    audit(AuditData(
      action = "view",
      entity = view,
      entity_id = id,
      user = user,
      oldData = Map.empty[String, Any],
      newData = data)
    )
  }

   def logList(view: String, user: User, inParams: Map[String, Any], offset: Int, limit: Int, orderBy: String, state: ApplicationState, doCount: Boolean): Unit = {
     audit(AuditData(
       action = "list",
       entity = view,
       user = user,
       newData = Map[String, Any](
         "params" -> removeBlacklistedFields(inParams),
         "offset" -> offset, "limit"->limit, "orderBy"->orderBy,
         "state"-> removeBlacklistedFields(state),
         "doCount"->doCount))
     )
   }

   override def auditSave(id: jLong, viewName: String, map: Map[String, Any], error: String)(implicit user: User, state: ApplicationState): Unit = {
     val relevantIds = getRelevantIds(viewName, id, map, loadFromDb = false, null)
     audit(AuditData(action = "create", entity_id = id, user = user, time = now, entity = viewName,
       newData = map, relevant_id = relevantIds, error = error))
   }

   def logSave(id: Long, view: String, old: qe.DTO, obj: qe.DTO, inParams: Map[String, Any],
               user: User, save: Map[String, Any], extraPropsToSave: Map[String, Any], error: Throwable): Unit = {
     val newData = mapFromObj(obj)
     val oldData = mapFromObj(old)
     val relevantIds = getRelevantIds(view, id, newData ++ Option(extraPropsToSave).getOrElse(Map.empty), error == null, error)
     val diff = getDiff(oldData, newData)
     if (diff.nonEmpty || logUnchangedSaves)
       audit(AuditData(action = if (old == null) "create" else "save", entity_id = id, user = user, time = now, entity = view,
         newData = newData, oldData = oldData, diff = diff, relevant_id = relevantIds, error = Option(error).map(_.getMessage).orNull))
   }

   def logRemove(id: Long, view: String, user: User, oldData: qe.DWI, error: Throwable): Unit = {
     val data = mapFromObj(oldData)
     audit(AuditData(action = "remove", entity = view, entity_id = id, user = user, time = now,
       oldData = data, relevant_id = getRelevantIds(view, id, data, error != null, error), error = Option(error).map(_.getMessage).orNull))
   }

   def audit[C <: RequestContext[_]](res: C, error: Throwable): Unit = {
     res match {
       case _: CreateContext[_] => // IGNORE
       case ViewContext(view, id, inParams, user, state, result) =>
         logView(id, view, user, inParams, state,
           Option(result)
             .flatten
             .filter(_ != null)
             .filter(_.isInstanceOf[qe.DWI@unchecked])
             .map(_.asInstanceOf[qe.DWI])
             .map(v => mapFromObj(v))
             .getOrElse(Map.empty[String, Any]))
       case ListContext(view, inParams, offset, limit, orderBy, user, state, _, doCount, _, _, _, _, _) =>
         logList(view, user, inParams, offset, limit, orderBy, state, doCount)
       case SaveContext(view, old, obj, inParams, user, _, state, extraPropsToSave, result) =>
         if (old != null || error == null) logSave( //do not audit error on insert
           Option(old).
             filter(_.isInstanceOf[qe.DWI@unchecked])
             .map(_.asInstanceOf[qe.DWI])
             .map(_.id.toLong)
             .getOrElse(result),
           view,
           old.asInstanceOf[qe.DWI],
           obj.asInstanceOf[qe.DWI],
           inParams,
           user,
           state,
           extraPropsToSave,
           error
         )
       case RemoveContext(view, id, inParams, user, _, _, result, oldData) =>
         logRemove(id, view, user, oldData.asInstanceOf[qe.DWI], error)
     }
   }
 }
}
