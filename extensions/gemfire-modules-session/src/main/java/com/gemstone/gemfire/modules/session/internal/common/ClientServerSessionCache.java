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

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.modules.util.BootstrappingFunction;
import com.gemstone.gemfire.modules.util.CreateRegionFunction;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import com.gemstone.gemfire.modules.util.RegionStatus;

import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which defines a client/server cache.
 */
public class ClientServerSessionCache extends AbstractSessionCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(PeerToPeerSessionCache.class.getName());

  private ClientCache cache;

  protected static final String DEFAULT_REGION_ATTRIBUTES_ID =
      RegionShortcut.PARTITION_REDUNDANT.toString();

  protected static final Boolean DEFAULT_ENABLE_LOCAL_CACHE = true;

  /**
   * Constructor
   *
   * @param cache
   * @param properties
   */
  public ClientServerSessionCache(ClientCache cache,
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

  @Override
  public void initialize() {
    // Bootstrap the servers
    bootstrapServers();

    // Create or retrieve the region
    createOrRetrieveRegion();

    // Set the session region directly as the operating region since there is no difference
    // between the local cache region and the session region.
    operatingRegion = sessionRegion;

    // Create or retrieve the statistics
    createStatistics();
  }

  @Override
  public GemFireCache getCache() {
    return cache;
  }

  @Override
  public boolean isClientServer() {
    return true;
  }


  ////////////////////////////////////////////////////////////////////////
  // Private methods

  private void bootstrapServers() {
    Execution execution = FunctionService.onServers(this.cache);
    ResultCollector collector = execution.execute(new BootstrappingFunction());
    // Get the result. Nothing is being done with it.
    try {
      collector.getResult();
    } catch (Exception e) {
      // If an exception occurs in the function, log it.
      LOG.warn("Caught unexpected exception:", e);
    }
  }

  private void createOrRetrieveRegion() {
    // Retrieve the local session region
    this.sessionRegion =
        this.cache.getRegion(
            (String) properties.get(CacheProperty.REGION_NAME));

    // If necessary, create the regions on the server and client
    if (this.sessionRegion == null) {
      // Create the PR on the servers
      createSessionRegionOnServers();

      // Create the region on the client
      this.sessionRegion = createLocalSessionRegion();
      LOG.debug("Created session region: " + this.sessionRegion);
    } else {
      LOG.debug("Retrieved session region: " + this.sessionRegion);
    }
  }

  private void createSessionRegionOnServers() {
    // Create the RegionConfiguration
    RegionConfiguration configuration = createRegionConfiguration();

    // Send it to the server tier
    Execution execution = FunctionService.onServer(this.cache).withArgs(
        configuration);
    ResultCollector collector = execution.execute(CreateRegionFunction.ID);

    // Verify the region was successfully created on the servers
    List<RegionStatus> results = (List<RegionStatus>) collector.getResult();
    for (RegionStatus status : results) {
      if (status == RegionStatus.INVALID) {
        StringBuilder builder = new StringBuilder();
        builder.append(
            "An exception occurred on the server while attempting to create or validate region named ");
        builder.append(properties.get(CacheProperty.REGION_NAME));
        builder.append(". See the server log for additional details.");
        throw new IllegalStateException(builder.toString());
      }
    }
  }

  private Region<String, HttpSession> createLocalSessionRegion() {
    ClientRegionFactory<String, HttpSession> factory = null;
    boolean enableLocalCache =
        (Boolean) properties.get(CacheProperty.ENABLE_LOCAL_CACHE);

    String regionName = (String) properties.get(CacheProperty.REGION_NAME);
    if (enableLocalCache) {
      // Create the region factory with caching and heap LRU enabled
      factory = ((ClientCache) this.cache).
          createClientRegionFactory(
              ClientRegionShortcut.CACHING_PROXY_HEAP_LRU);
      LOG.info("Created new local client session region: {}", regionName);
    } else {
      // Create the region factory without caching enabled
      factory = ((ClientCache) this.cache).createClientRegionFactory(
          ClientRegionShortcut.PROXY);
      LOG.info(
          "Created new local client (uncached) session region: {} without any session expiry",
          regionName);
    }

    // Create the region
    return factory.create(regionName);
  }
}
