/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina.callback;

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import javax.servlet.http.HttpSession;

public class LocalSessionCacheLoader implements CacheLoader<String, HttpSession>,
    Declarable {

  private final Region<String,HttpSession> backingRegion;
  
  public LocalSessionCacheLoader(Region<String,HttpSession> backingRegion) {
    this.backingRegion = backingRegion;
  }
  
  public HttpSession load(LoaderHelper<String,HttpSession> helper) throws CacheLoaderException {
    return this.backingRegion.get(helper.getKey());
  }

  public void close() {}

  public void init(Properties p) {}
}