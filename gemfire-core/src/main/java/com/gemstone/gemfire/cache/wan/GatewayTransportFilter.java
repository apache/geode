/*
 * =========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.cache.wan;

import java.io.InputStream;
import java.io.OutputStream;

import com.gemstone.gemfire.cache.CacheCallback;

public interface GatewayTransportFilter extends CacheCallback {
  public InputStream  getInputStream(InputStream stream);
  public OutputStream getOutputStream(OutputStream stream);
}
