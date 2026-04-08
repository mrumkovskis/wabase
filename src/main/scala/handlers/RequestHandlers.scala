package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.{StatusCodes, Uri}
import org.apache.pekko.http.scaladsl.model.headers.Cookie
import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.wabase.AppMetadata.Action
import org.wabase.{AppServiceBase, ApplicationState, I18nService, WabaseRequestContext, WabaseService, config}
import org.wabase.WabaseService.error

import java.util.Locale

object RequestHandlers {

  /** Enables alternative URI where row key is in special query string */
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
        AppServiceBase.escapeReflectedXss)(c.name, c.value)
    }.toMap
    val langKey = prefix + I18nService.ApplicationLanguageCookiePostfix
    if (state.contains(langKey))
      ApplicationState(state, new Locale(String.valueOf(state(langKey))))
    else
      I18nService.currentLangFromHeader(ctx.req)
        .map(l => ApplicationState(state + (langKey -> l), new Locale(l)))
        .getOrElse(ApplicationState(state))
  }

  val CreateCountActionAndViewRegex = """(?U)(?:(count|create):)?([_\p{IsLatin}][\-\w]*)""".r
  val ActionForHttpPost = config.getString("app.action-for-http.post") // maybe "insert" for legacy app
  val ActionForHttpPut  = config.getString("app.action-for-http.put")  // maybe "update" for legacy app
  def viewActionKey(view_action: String, ctx: WabaseRequestContext): WabaseRequestContext = {
    import ctx._
    val viewDefs = wabase.qe.nameToViewDef
    val (viewNameAndActionStr, view_name, create_count_action) = try {
      val CreateCountActionAndViewRegex(cca, vn) = view_action
      if (viewDefs.contains(vn)) (view_action, vn, cca)
      else (null, null, null)
    } catch {
      case ex: scala.MatchError =>
        throw new RuntimeException(s"Unsupported view_name: $view_action", ex)
    }

    if (viewNameAndActionStr == null) ctx
    else {
      val key = WabaseService.key(req.uri.path, viewNameAndActionStr)
      val action = if (create_count_action != null) create_count_action else req.method match {
        case `GET`    => Action.Get
        case `POST`   => ActionForHttpPost
        case `PUT`    => ActionForHttpPut
        case `DELETE` => Action.Delete
        case `HEAD`   => Action.Head
        case `OPTIONS`=> Action.Options
        case x        => error(StatusCodes.MethodNotAllowed, s"Unsupported http method $x for request '${req.uri}'")
      }
      val apiAction = wabase.apiMethod(viewDefs(view_name), action, key.size)
      ctx.copy(viewName = view_name, action = apiAction, key = key)
    }
  }
}
