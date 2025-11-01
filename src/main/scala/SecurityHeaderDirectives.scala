package org.wabase

import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Directive0

trait SecurityHeaderDirectives {
  
  // Protect against click-jacking
  def frameHeader(option: String = "SAMEORIGIN"): Directive0 = 
    respondWithHeaders(
      RawHeader("X-Frame-Options", option)
    )
    
  // Basic XSS prevention
  def xssHeaders: Directive0 = 
    respondWithHeaders(
      RawHeader("X-XSS-Protection", "1; mode=block"),
      RawHeader("X-Content-Type-Options", "nosniff")      
    )
  
  // Force browser to use only https
  // Default - 1 year
  def hstsHeaders(maxAge: Long = 365 * 24 * 60 * 60, includeSubDomains: Boolean = true): Directive0 = 
    respondWithHeaders(
      `Strict-Transport-Security`(maxAge, includeSubDomains)
    )

  // Don't store response, always request new data
  // The way to do it, according to MDN:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#Preventing_caching
  def noCacheHeaders: Directive0 = 
    respondWithHeaders(
      `Cache-Control`(CacheDirectives.`no-cache`, CacheDirectives.`no-store`, CacheDirectives.`must-revalidate`)
    )
    
}