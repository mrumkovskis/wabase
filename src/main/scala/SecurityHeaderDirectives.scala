package org.wabase

import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Directives._

trait SecurityHeaderDirectives {
  
  // Protect against click-jacking
  def frameHeader(option: String = "SAMEORIGIN") = 
    respondWithHeaders(
      RawHeader("X-Frame-Options", option)
    )
    
  // Basic XSS prevention
  def xssHeaders = 
    respondWithHeaders(
      RawHeader("X-XSS-Protection", "1; mode=block"),
      RawHeader("X-Content-Type-Options", "nosniff")      
    )
  
  // Force browser to use only https
  // Default - 1 year
  def hstsHeaders(maxAge: Long = 365 * 24 * 60 * 60, includeSubDomains: Boolean = true) = 
    respondWithHeaders(
      `Strict-Transport-Security`(maxAge, includeSubDomains)
    )

  // Don't store response, always request new data
  // The way to do it, according to MDN:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control#Preventing_caching
  def noCacheHeaders = 
    respondWithHeaders(
      `Cache-Control`(CacheDirectives.`no-cache`, CacheDirectives.`no-store`, CacheDirectives.`must-revalidate`)
    )
    
}