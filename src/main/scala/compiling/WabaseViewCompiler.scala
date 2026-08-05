package org.wabase.compiling

import org.mojoz.metadata.ViewDef
import org.mojoz.querease.QueryStringBuilder.CompilationUnit
import org.mojoz.querease.compiling.ViewCompiler
import org.wabase.AppMetadata.Action.TresqlExtraction.{OpTresqlTraverser, State, StepTresqlTraverser, opTresqlTraverser, stepTresqlTraverser}
import org.wabase.{AppMetadata, AppQuerease, ClassLoaderTresqlResourcesConf, Macros, TresqlResourcesConf}
import org.wabase.AppMetadata._

import scala.collection.immutable.{Map, Seq, Set}
import scala.jdk.CollectionConverters._

trait WabaseViewCompiler extends ViewCompiler with AppMetadata { this: AppQuerease =>

  override lazy val macrosClass: Class[_] = {
    val cl = resourceClassLoader
    if (cl == null) TresqlResourcesConf.confs.get(null)
      .flatMap(c => Option(c.macros))
      .map(_.getClass)
      .getOrElse(classOf[Macros])
    else new ClassLoaderTresqlResourcesConf(resourceClassLoader).confs.get(null)
      .flatMap(c => Option(c.macros))
      .map(_.getClass)
      .getOrElse(classOf[Macros])
  }

  override protected def isActionCacheUpdatable: Boolean = true

  protected def actionQueries(actionName: String, objName: String, action: Action): scala.collection.mutable.Set[(String,String)] = {
    case class QueriesState(dbStack: List[String], queries: scala.collection.mutable.Set[(String, String)])
    lazy val opTresqlTrav: OpTresqlTraverser[QueriesState] =
      opTresqlTraverser(opTresqlTrav, stepTresqlTrav)(st => {
        case Action.Db(action, _, dbk :: _) =>
          Action.traverseAction(action)(stepTresqlTrav)(
            st.copy(value = st.value.copy(dbStack = dbk.db :: st.value.dbStack))
          )
          st
        case Action.ViewCall(_, _, data, _) => opTresqlTrav(st)(data) // do not go to process view call since all views are compiled
      })
    lazy val stepTresqlTrav: StepTresqlTraverser[QueriesState] =
      stepTresqlTraverser(opTresqlTrav, stepTresqlTrav)(st => {
        case Action.Validations(_, validations, db) =>
          val v = viewDef(st.name)
          validationsQueryString(v, validations).flatMap { valStr =>
            db.flatMap(k => Option(k.db)).map("|" + _ + ":" + valStr)
              .orElse(Option(valStr))
              .map { tresql =>
                st.copy(value = st.tresqlExtractor(st.value)(Action.Tresql(tresql)))
              }
          }.getOrElse(st)
      })
    val state = State[QueriesState](
      actionName, objName, Map(),
      tresqlExtractor = cq => tresql => {
        val tresqlString = tresql.tresql
        cq.queries += (cq.dbStack.headOption.orNull -> tresqlString)
        cq
      },
      viewExtractor = v => _ => v,
      // use linked hash set to preserve query order
      processed = Set(), value = QueriesState(Nil, scala.collection.mutable.LinkedHashSet[(String, String)]())
    )
    Action.traverseAction(action)(stepTresqlTrav)(state).value.queries
  }
  override def allQueryStrings(viewDef: ViewDef): Seq[CompilationUnit] = {
    super.allQueryStrings(viewDef) ++ viewDef.actions.flatMap { case (actionName, action) =>
      val objName = viewDef.name
      actionQueries(actionName, objName, action)
        .map(compilationUnit("action-queries", s"$objName.$actionName", viewDef.db, _))
    }
  }

  private def compilationUnit(category: String, source: String, defaultDb: String, query: (String, String)) = {
    val (db, q) = query
    CompilationUnit(category, source, if (db == null) defaultDb else db, q)
  }

  override protected def serializedCaches: Map[String, Array[Byte]] = {
    import io.bullet.borer._

    import org.wabase.CacheIo.varCodec
    val serializedVars = Map(
      ViewNameToQueryVariablesCacheName -> Cbor.encode(viewNameToQueryVariablesCompilerCache.asScala.toMap).toByteArray)

    super.serializedCaches ++ serializedVars
  }

  /** Clear all caches used for query compilation */
  override protected def clearCompilerCaches(): Unit = {
    super.clearCompilerCaches()
    viewNameToQueryVariablesCompilerCache.clear()
  }
}
