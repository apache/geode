/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.http;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * The HttpHeader enum is an enumeration of all HTTP request/response header names.
 * <p/>
 * @author John Blum
 * @link http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html
 * @since 8.0
 */
@SuppressWarnings("unused")
public enum HttpHeader {
  ACCEPT("Accept"),
  ACCEPT_CHARSET("Accept-Charset"),
  ACCEPT_ENCODING("Accept-Encoding"),
  ACCEPT_LANGUAGE("Accept-Language"),
  ACCEPT_RANGES("Accept-Ranges"),
  AGE("Age"),
  ALLOW("Allow"),
  AUTHORIZATION("Authorization"),
  CACHE_CONTROL("Cache-Control"),
  CONNECTION("Connection"),
  CONTENT_ENCODING("Content-Encoding"),
  CONTENT_LANGUAGE("Content-Language"),
  CONTENT_LENGTH("Content-Length"),
  CONTENT_LOCATION("Content-Location"),
  CONTENT_MD5("Content-MD5"),
  CONTENT_RANGE("Content-Range"),
  CONTENT_TYPE("Content-Type"),
  DATE("Date"),
  ETAG("ETag"),
  EXPECT("Expect"),
  EXPIRES("Expires"),
  FROM("From"),
  HOST("Host"),
  IF_MATCH("If-Match"),
  IF_MODIFIED_SINCE("If-Modified-Since"),
  IF_NONE_MATCH("If-None-Match"),
  IF_RANGE("If-Range"),
  IF_UNMODIFIED_SINCE("If-Unmodified-Since"),
  LAST_MODIFIED("Last-Modified"),
  LOCATION("Location"),
  MAX_FORWARDS("Max-Forwards"),
  PRAGMA("Pragma"),
  PROXY_AUTHENTICATE("Proxy-Authenticate"),
  PROXY_AUTHORIZATION("Proxy-Authorization"),
  RANGE("Range"),
  REFERER("Referer"),
  RETRY_AFTER("Retry-After"),
  SERVER("Server"),
  TE("TE"),
  TRAILER("Trailer"),
  TRANSFER_ENCODING("Transfer-Encoding"),
  UPGRADE("Upgrade"),
  USER_AGENT("User-Agent"),
  VARY("Vary"),
  VIA("Via"),
  WARNING("Warning"),
  WWW_AUTHENTICATE("WWW-Authenticate");

  // the name of the Http request or response header
  private final String name;

  HttpHeader(final String name) {
    assert !StringUtils.isBlank(name) : "The name of the HTTP request header must be specified!";
    this.name = name;
  }

  public static HttpHeader valueOfName(final String name) {
    for (final HttpHeader header : values()) {
      if (header.getName().equalsIgnoreCase(name)) {
        return header;
      }
    }

    return null;
  }

  public String getName() {
    return name;
  }

}
