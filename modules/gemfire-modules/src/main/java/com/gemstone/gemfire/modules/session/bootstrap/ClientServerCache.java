/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.bootstrap;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;

/**
 * This is a singleton class which maintains configuration properties as well
 * as starting a Client-Server cache.
 */
public class ClientServerCache extends AbstractCache {

  protected static final String DEFAULT_CACHE_XML_FILE_NAME = "cache-client.xml";

  static {
    instance = new ClientServerCache();
  }

  private ClientServerCache() {
      // Singleton
      super();
  }

  public static AbstractCache getInstance() {
      return instance;
  }

  @Override
  protected void createOrRetrieveCache() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug(this + ": Creating cache");
    }
    // Get the existing cache if any
    try {
      this.cache = ClientCacheFactory.getAnyInstance();
    } catch (CacheClosedException e) {}
    
    // If no cache exists, create one
    String message = null;
    if (this.cache == null) {
      // enable pool subscription so that default cache can be used by hibernate module
      this.cache = new ClientCacheFactory(createDistributedSystemProperties()).create();
      message = "Created ";
    } else {
      message = "Retrieved ";
    }
    getLogger().info(message + this.cache);
  }
  
  @Override
  protected void rebalanceCache() {
    getLogger().warn("The client cannot rebalance the server's cache.");
  }
  
  @Override
  protected String getDefaultCacheXmlFileName() {
    return DEFAULT_CACHE_XML_FILE_NAME;
  }
}
