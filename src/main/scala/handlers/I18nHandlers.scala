package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.headers.{HttpCookie, SameSite}
import org.apache.pekko.http.scaladsl.model.{HttpResponse, Uri}
import org.wabase._
import org.wabase.I18nService.i18BundleMarshaller

import java.nio.charset.StandardCharsets
import scala.concurrent.Future

object I18nHandlers {

  def setLanguage(lang: String, resp: HttpResponse): HttpResponse = {
    WabaseService.setCookie(resp)(
      HttpCookie(AppServiceBase.ApplicationStateCookiePrefix + I18nService.ApplicationLanguageCookiePostfix,
        value = lang,
        path = Some("/")
      ).withSameSite(SameSite.Lax)
    )
  }

  def i18nTranslate(name: String, key: String, params: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    implicit val locale = I18nService.applicationLocale(ctx.applicationState)
    val paramsSeq   = Option(params).map(Uri.Path.apply(_, StandardCharsets.UTF_8)).map(WabaseService.pathSegments).getOrElse(Nil)
    val translation = ctx.wabase.translateFromBundle(name, key, paramsSeq: _*)
    WabaseService.complete(ctx, translation)
  }

  def i18nResources(ctx: WabaseRequestContext): Future[HttpResponse] = {
    implicit val locale = I18nService.applicationLocale(ctx.applicationState)
    val res = ctx.wabase.i18nResources
    WabaseService.complete(ctx, res)
  }

  def i18nResourcesFromBundle(bundleName: String, ctx: WabaseRequestContext): Future[HttpResponse] = {
    implicit val locale = I18nService.applicationLocale(ctx.applicationState)
    val translation = ctx.wabase.i18nResourcesFromBundle(bundleName)
    WabaseService.complete(ctx, translation)
  }
}
