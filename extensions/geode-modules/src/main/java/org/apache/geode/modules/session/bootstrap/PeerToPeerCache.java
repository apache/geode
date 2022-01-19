/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.modules.session.bootstrap;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.modules.util.RegionHelper;

/**
 * This is a singleton class which maintains configuration properties as well as starting a
 * Peer-To-Peer cache.
 */

public class PeerToPeerCache extends AbstractCache {
  private static final String DEFAULT_CACHE_XML_FILE_NAME = "cache-peer.xml";

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
      cache = CacheFactory.getAnyInstance();
    } catch (CacheClosedException ignored) {
    }

    // If no cache exists, create one
    String message;
    if (cache == null || cache.isClosed()) {
      cache = new CacheFactory(createDistributedSystemProperties()).create();
      message = "Created ";
    } else {
      message = "Retrieved ";
    }
    getLogger().info(message + cache);
  }

  @Override
  protected void rebalanceCache() {
    try {
      getLogger().info("Rebalancing: " + cache);
      RebalanceResults results = RegionHelper.rebalanceCache(cache);
      if (getLogger().isDebugEnabled()) {
        getLogger().debug("Done rebalancing: " + cache);
        getLogger().debug(RegionHelper.getRebalanceResultsMessage(results));
      }
    } catch (Exception e) {
      getLogger().warn("Rebalance failed because of the following exception:", e);
    }
  }

  @Override
  protected String getDefaultCacheXmlFileName() {
    return DEFAULT_CACHE_XML_FILE_NAME;
  }
}
