package org.wabase.compiling

import org.mojoz.metadata.ViewDef
import org.mojoz.querease.QueryStringBuilder.CompilationUnit
import org.mojoz.querease.compiling.ViewCompiler
import org.tresql.{MacroResourcesImpl, QueryParser, SimpleCache, ast}
import org.wabase.AppMetadata.Action.TresqlExtraction.{OpTresqlTraverser, State, StepTresqlTraverser, opTresqlTraverser, stepTresqlTraverser}
import org.wabase.{AppMetadata, AppQuerease, ClassLoaderTresqlResourcesConf, Macros, TresqlResourcesConf, getObjectOrNewInstance}
import org.wabase.AppMetadata._

import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable.{Map, Seq, Set}
import scala.jdk.CollectionConverters._

trait WabaseViewCompiler extends ViewCompiler with AppMetadata { this: AppQuerease =>

  private lazy val viewNameToQueryVariablesCompilerCache = {
    val cache = new ConcurrentHashMap[String, Seq[ast.Variable]]
    cache.putAll(viewNameToQueryVariablesCache.asJava)
    cache
  }

  override lazy val macrosClass: Class[_] = {
    val cl = resourcesClassLoader
    if (cl == null) TresqlResourcesConf.confs.get(null)
      .flatMap(c => Option(c.macros))
      .map(_.getClass)
      .getOrElse(classOf[Macros])
    else new ClassLoaderTresqlResourcesConf(resourcesClassLoader).confs.get(null)
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
      stepTresqlTraverser(opTresqlTrav)(st => {
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

  override protected def compileQueries(
    category: String,
    compilationUnits: Seq[CompilationUnit],
    previouslyCompiledQueries: Set[String],
    showFailedViewQuery: Boolean,
    log: => String => Unit,
  ): Int = category match {
    case "queries" =>
      log(s"Compiling $category - ${compilationUnits.size} total")
      val startTime = System.currentTimeMillis
      val dbToCompiler = compilationUnits.map(_.db).toSet.map { (db: String) =>
        val compiler = new QueryParser(macroResources, new SimpleCache(parserCacheSize)) with org.tresql.compiling.Compiler {
          override val metadata = if (db == null) tresqlMetadata else tresqlMetadata.extraDbToMetadata(db)
          override val extraMetadata = tresqlMetadata.extraDbToMetadata
        }
        db -> compiler
      }.toMap
      val compiledQueries = collection.mutable.Set[String](previouslyCompiledQueries.toSeq: _*)
      var compiledCount = 0
      compilationUnits.foreach { case cu @ CompilationUnit(_, viewName, db, q) =>
        if (!compiledQueries.contains(cu.queryStringWithContext) ||
          viewNameToQueryVariablesCompilerCache.get(viewName) == null) {
          val compiler = dbToCompiler(db)
          try compiler.compile(compiler.parseExp(q)) catch { case util.control.NonFatal(ex) =>
            val msg = s"\nFailed to compile $viewName query: ${ex.getMessage}" +
              (if (showFailedViewQuery) s"\n$q" else "")
            throw new RuntimeException(msg, ex)
          }
          viewNameToQueryVariablesCompilerCache.put(viewName, compiler.extractVariables(q))
          if (!compiledQueries.contains(cu.queryStringWithContext)) {
            compiledCount += 1
            compiledQueries += cu.queryStringWithContext
          }
        }
      }
      val endTime = System.currentTimeMillis
      val allQueries = compilationUnits.map(_.queryStringWithContext).toSet
      log(
        s"Query compilation done - ${endTime - startTime} ms, " +
          s"queries compiled: $compiledCount" +
          (if (compiledCount != allQueries.size) s" of ${allQueries.size}" else ""))
      compiledCount
    case _ => super.compileQueries(
      category, compilationUnits, previouslyCompiledQueries, showFailedViewQuery, log)
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
