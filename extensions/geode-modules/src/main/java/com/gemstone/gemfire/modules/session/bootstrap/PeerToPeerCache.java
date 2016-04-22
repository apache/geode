/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gemstone.gemfire.modules.session.bootstrap;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.modules.util.RegionHelper;

/**
 * This is a singleton class which maintains configuration properties as well as starting a Peer-To-Peer cache.
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
      this.cache = new CacheFactory(createDistributedSystemProperties()).create();
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
      getLogger().warn("Rebalance failed because of the following exception:", e);
    }
  }

  @Override
  protected String getDefaultCacheXmlFileName() {
    return DEFAULT_CACHE_XML_FILE_NAME;
  }
}
