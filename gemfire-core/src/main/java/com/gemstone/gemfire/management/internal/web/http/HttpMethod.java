/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.http;

/**
 * The HttpMethod enum is an enumeration of all HTTP methods (POST, GET, PUT, DELETE, HEADERS, etc).
 * <p/>
 * @author John Blum
 * @since 8.0
 */
@SuppressWarnings("unused")
public enum HttpMethod {
  CONNECT,
  DELETE,
  GET,
  HEAD,
  OPTIONS,
  POST,
  PUT,
  TRACE

}
