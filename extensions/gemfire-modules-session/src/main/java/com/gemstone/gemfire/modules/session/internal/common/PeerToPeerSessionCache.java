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

package com.gemstone.gemfire.modules.session.internal.common;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.modules.session.catalina.callback.LocalSessionCacheLoader;
import com.gemstone.gemfire.modules.session.catalina.callback.LocalSessionCacheWriter;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import com.gemstone.gemfire.modules.util.RegionHelper;
import com.gemstone.gemfire.modules.util.TouchPartitionedRegionEntriesFunction;
import com.gemstone.gemfire.modules.util.TouchReplicatedRegionEntriesFunction;

import java.util.Map;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which defines a peer-to-peer cache
 */
public class PeerToPeerSessionCache extends AbstractSessionCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(PeerToPeerSessionCache.class.getName());

  private Cache cache;

  private static final String DEFAULT_REGION_ATTRIBUTES_ID =
      RegionShortcut.REPLICATE.toString();

  private static final Boolean DEFAULT_ENABLE_LOCAL_CACHE = false;

  /**
   * Constructor
   *
   * @param cache
   * @param properties
   */
  public PeerToPeerSessionCache(Cache cache,
      Map<CacheProperty, Object> properties) {
    super();
    this.cache = cache;

    /**
     * Set some default properties for this cache if they haven't already
     * been set
     */
    this.properties.put(CacheProperty.REGION_ATTRIBUTES_ID,
        DEFAULT_REGION_ATTRIBUTES_ID);
    this.properties.put(CacheProperty.ENABLE_LOCAL_CACHE,
        DEFAULT_ENABLE_LOCAL_CACHE);
    this.properties.putAll(properties);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize() {
    // Register Functions
    registerFunctions();

    // Create or retrieve the region
    createOrRetrieveRegion();

    /**
     * If local cache is enabled, create the local region fronting the
     * session region and set it as the operating region; otherwise, use
     * the session region directly as the operating region.
     */
    boolean enableLocalCache =
        (Boolean) properties.get(CacheProperty.ENABLE_LOCAL_CACHE);
    operatingRegion = enableLocalCache
        ? createOrRetrieveLocalRegion()
        : this.sessionRegion;

    // Create or retrieve the statistics
    createStatistics();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GemFireCache getCache() {
    return cache;
  }

  @Override
  public boolean isClientServer() {
    return false;
  }

  private void registerFunctions() {
    // Register the touch partitioned region entries function if it is not already registered
    if (!FunctionService.isRegistered(
        TouchPartitionedRegionEntriesFunction.ID)) {
      FunctionService.registerFunction(
          new TouchPartitionedRegionEntriesFunction());
    }

    // Register the touch replicated region entries function if it is not already registered
    if (!FunctionService.isRegistered(
        TouchReplicatedRegionEntriesFunction.ID)) {
      FunctionService.registerFunction(
          new TouchReplicatedRegionEntriesFunction());
    }
  }

  private void createOrRetrieveRegion() {
    // Create the RegionConfiguration
    RegionConfiguration configuration = createRegionConfiguration();

    // Attempt to retrieve the region
    // If it already exists, validate it
    // If it doesn't already exist, create it
    Region region = this.cache.getRegion(
        (String) properties.get(CacheProperty.REGION_NAME));
    if (region == null) {
      // Create the region
      region = RegionHelper.createRegion(cache, configuration);
      LOG.info("Created new session region: {}", region);
    } else {
      // Validate the existing region
      LOG.info("Retrieved existing session region: {}", region);
      RegionHelper.validateRegion(cache, configuration, region);
    }

    // Set the session region
    this.sessionRegion = region;
  }

  /**
   * Create a local region fronting the main region.
   *
   * @return
   */
  private Region<String, HttpSession> createOrRetrieveLocalRegion() {
    // Attempt to retrieve the fronting region
    String frontingRegionName = this.sessionRegion.getName() + "_local";
    Region<String, HttpSession> frontingRegion =
        this.cache.getRegion(frontingRegionName);

    if (frontingRegion == null) {
      // Create the region factory
      RegionFactory<String, HttpSession> factory =
          this.cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);

      // Add the cache loader and writer
      factory.setCacheLoader(new LocalSessionCacheLoader(this.sessionRegion));
      factory.setCacheWriter(new LocalSessionCacheWriter(this.sessionRegion));

      // Create the region
      frontingRegion = factory.create(frontingRegionName);
      LOG.info("Created new local session region: {}", frontingRegion);
    } else {
      LOG.info("Retrieved existing local session region: {}",
          frontingRegion);
    }

    return frontingRegion;
  }
}
