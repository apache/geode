/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.bootstrap;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.modules.util.RegionHelper;

/**
 * This is a singleton class which maintains configuration properties as well as
 * starting a Peer-To-Peer cache.
 */

public class PeerToPeerCache extends AbstractCache {

  protected static final String DEFAULT_CACHE_XML_FILE_NAME = "cache-peer.xml";

  static {
    instance = new PeerToPeerCache();
  }

  private PeerToPeerCache() {
    // Singleton
    super();
  }

  public static AbstractCache getInstance() {
    return instance;
  }

  @Override
  protected void createOrRetrieveCache() {
    if (getLogger().isDebugEnabled()) {
      getLogger().debug("Creating cache");
    }
    // Get the existing cache if any
    try {
      this.cache = CacheFactory.getAnyInstance();
    } catch (CacheClosedException e) {
    }

    // If no cache exists, create one
    String message = null;
    if (this.cache == null) {
      this.cache = new CacheFactory(
          createDistributedSystemProperties()).create();
      message = "Created ";
    } else {
      message = "Retrieved ";
    }
    getLogger().info(message + this.cache);
  }

  @Override
  protected void rebalanceCache() {
    try {
      getLogger().info("Rebalancing: " + this.cache);
      RebalanceResults results = RegionHelper.rebalanceCache(this.cache);
      if (getLogger().isDebugEnabled()) {
        getLogger().debug("Done rebalancing: " + this.cache);
        getLogger().debug(RegionHelper.getRebalanceResultsMessage(results));
      }
    } catch (Exception e) {
      getLogger().warn("Rebalance failed because of the following exception:",
          e);
    }
  }

  @Override
  protected String getDefaultCacheXmlFileName() {
    return DEFAULT_CACHE_XML_FILE_NAME;
  }
}
