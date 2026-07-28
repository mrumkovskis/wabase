package org.wabase.handlers

import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, RawHeader, `Cache-Control`, `Strict-Transport-Security`}

/** Handler-chain counterpart of [[org.wabase.SecurityHeaderDirectives]] - adds security headers to the
  * response in the wabase RequestHandler pipeline (as opposed to pekko-http directives). */
object SecurityHeaderHandlers {

  // Protect against click-jacking
  def frameHeader(option: String = "SAMEORIGIN")(resp: HttpResponse): HttpResponse =
    resp.addHeader(RawHeader("X-Frame-Options", option))

  // Basic XSS prevention
  def xssHeaders(resp: HttpResponse): HttpResponse =
    resp.withHeaders(resp.headers ++ List(
      RawHeader("X-XSS-Protection", "1; mode=block"),
      RawHeader("X-Content-Type-Options", "nosniff"),
    ))

  // Force browser to use only https
  // Default - 1 year
  def hstsHeaders(maxAge: Long = 365 * 24 * 60 * 60, includeSubDomains: Boolean = true)(resp: HttpResponse): HttpResponse =
    resp.addHeader(`Strict-Transport-Security`(maxAge, includeSubDomains))

  // Don't store response, always request new data
  // The way to do it, according to MDN:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#Preventing_caching
  def noCacheHeaders(resp: HttpResponse): HttpResponse =
    resp.addHeader(`Cache-Control`(CacheDirectives.`no-cache`, CacheDirectives.`no-store`, CacheDirectives.`must-revalidate`))
}
