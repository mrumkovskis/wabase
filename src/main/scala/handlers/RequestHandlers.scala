package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.model.headers.Cookie
import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.wabase.AppMetadata.Action
import org.wabase.{AppQuerease, AppServiceBase, ApplicationState, I18nService, WabaseRequestContext, WabaseService, config}
import org.wabase.WabaseService.error

import java.util.Locale

object RequestHandlers {

  /** Moves key from special query string (?/key/parts) into the path when `app.key-in-query` is true.
    * Always records [[AppQuerease.OriginalRequestUriAttribute]] for relative redirect resolution. */
  def maybeKeyFromQueryToPath(ctx: WabaseRequestContext): WabaseRequestContext = {
    val withOriginal = ctx.copy(req =
      if (ctx.req.attribute(AppQuerease.OriginalRequestUriAttribute).isDefined) ctx.req
      else ctx.req.addAttribute(AppQuerease.OriginalRequestUriAttribute, ctx.req.uri))
    if (AppServiceBase.KeyInQuery) keyFromQueryToPath(withOriginal) else withOriginal
  }

  /** Moves key from special query string (?/key/parts) into the path. */
  def keyFromQueryToPath(ctx: WabaseRequestContext): WabaseRequestContext = {
    val request = ctx.req
    def decode(s: String) = java.net.URLDecoder.decode(s, "UTF-8")
    request.uri.rawQueryString match {
      case Some(rawQ) if rawQ startsWith "/" =>
        val (p, q) = rawQ.indexOf('?') match {
          case -1 => (rawQ, null)
          case i  => (rawQ.substring(0, i), rawQ.substring(i + 1))
        }
        @annotation.tailrec
        def toPath(path: Uri.Path, p: String): Uri.Path = p.indexOf('/', 1) match {
          case -1 => path / decode(p.substring(1))
          case i  => toPath(path / decode(p.substring(1, i)), p.substring(i))
        }
        val uriWithPath = request.uri.withPath(toPath(request.uri.path, p))
        val uri =
          if (q == null)
               uriWithPath.withQuery(Uri.Query.Empty)
          else uriWithPath.withRawQueryString(q)
        ctx.copy(req = request.withUri(uri))
      case _ => ctx
    }
  }

  def extractState(ctx: WabaseRequestContext): ApplicationState =
    extractStateForPrefix(AppServiceBase.ApplicationStateCookiePrefix, ctx)

  def extractStateForPrefix(prefix: String, ctx: WabaseRequestContext): ApplicationState = {
    val state = ctx.req.headers.flatMap {
      case c: Cookie => c.cookies.filter(_.name.startsWith(prefix))
      case _ => Nil
    }.map { c => c.name ->
      AppServiceBase.decodeParam(
        ctx.wabase.qe.metadataConventions,
        AppServiceBase.NamesForInts,
        AppServiceBase.escapeReflectedXss,
      )(c.name, c.value)
    }.toMap
    val langKey = prefix + I18nService.ApplicationLanguageCookiePostfix
    if (state.contains(langKey))
      ApplicationState(state, new Locale(String.valueOf(state(langKey))))
    else
      I18nService.currentLangFromHeader(ctx.req)
        .map(l => ApplicationState(state + (langKey -> l), new Locale(l)))
        .getOrElse(ApplicationState(state))
  }

  private def lastPathSegment(path: Uri.Path): Option[String] =
    WabaseService.pathSegments(path).lastOption

  private def matchedAllowedPath(allowedPaths: Seq[Uri.Path], requestPath: Uri.Path): Option[Uri.Path] =
    allowedPaths
      .filter(requestPath.startsWith)
      .sortBy(_.toString.length)
      .lastOption

  val NewCountActionAndViewRegex = """(?U)([_\p{IsLatin}][\-\w]*)(?::(count|new))?""".r
  val ActionForHttpPost = config.getString("app.action-for-http.post") // maybe "insert" for legacy app
  val ActionForHttpPut  = config.getString("app.action-for-http.put")  // maybe "update" for legacy app
  def viewActionKey(view_action: String, ctx: WabaseRequestContext): WabaseRequestContext = {
    import ctx._
    val viewDefs = wabase.qe.nameToViewDef
    val (viewNameAndActionStr, view_name, new_count_action) = try {
      val NewCountActionAndViewRegex(vn, cca) = view_action: @unchecked
      if (viewDefs.contains(vn)) (view_action, vn, cca)
      else (null, null, null)
    } catch {
      case ex: scala.MatchError =>
        throw new RuntimeException(s"Unsupported view_name: $view_action", ex)
    }

    if (viewNameAndActionStr == null) ctx
    else {
      val allowed   = wabase.qe.allowedPaths(view_name)
      val matched   = matchedAllowedPath(allowed, req.uri.path)
      val segment   = matched.flatMap(lastPathSegment(_)).getOrElse(viewNameAndActionStr)
      val key       = WabaseService.keyAfterSegment(req.uri.path, segment)
      val action = if (new_count_action != null) new_count_action else req.method match {
        case `GET`    => Action.Get
        case `POST`   => ActionForHttpPost
        case `PUT`    => ActionForHttpPut
        case `DELETE` => Action.Delete
        case `HEAD`   => Action.Head
        case `OPTIONS`=> Action.Options
        case x        => error(StatusCodes.MethodNotAllowed, s"Unsupported http method $x for request '${req.uri}'")
      }
      val apiAction = wabase.apiMethod(viewDefs(view_name), action, key.size)
      // Store root path (not count/new) for redirects; fall back to primary root when unmatched
      val viewApiPath =
        matched.filterNot(p => wabase.qe.isPathForCount(p) || wabase.qe.isPathForNew(p))
          .orElse(wabase.qe.rootPaths(view_name).headOption)
          .getOrElse(wabase.qe.primaryRootPath(view_name))
      val reqWithPath = req.addAttribute(AppQuerease.ViewApiPathAttribute, viewApiPath)
      ctx.copy(req = reqWithPath, viewName = view_name, action = apiAction, key = key)
    }
  }
}
