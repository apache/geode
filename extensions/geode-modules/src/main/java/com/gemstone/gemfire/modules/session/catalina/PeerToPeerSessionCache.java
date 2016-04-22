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
package com.gemstone.gemfire.modules.session.catalina;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.modules.session.catalina.callback.LocalSessionCacheLoader;
import com.gemstone.gemfire.modules.session.catalina.callback.LocalSessionCacheWriter;
import com.gemstone.gemfire.modules.session.catalina.callback.SessionExpirationCacheListener;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import com.gemstone.gemfire.modules.util.RegionHelper;
import com.gemstone.gemfire.modules.util.SessionCustomExpiry;
import com.gemstone.gemfire.modules.util.TouchPartitionedRegionEntriesFunction;
import com.gemstone.gemfire.modules.util.TouchReplicatedRegionEntriesFunction;

import javax.servlet.http.HttpSession;
import java.util.Set;

public class PeerToPeerSessionCache extends AbstractSessionCache {

  private Cache cache;

  protected static final String DEFAULT_REGION_ATTRIBUTES_ID = RegionShortcut.REPLICATE.toString();

  protected static final boolean DEFAULT_ENABLE_LOCAL_CACHE = false;

  public PeerToPeerSessionCache(SessionManager sessionManager, Cache cache) {
    super(sessionManager);
    this.cache = cache;
  }

  @Override
  public void initialize() {
    // Register Functions
    registerFunctions();

    // Create or retrieve the region
    createOrRetrieveRegion();

    // If local cache is enabled, create the local region fronting the session region
    // and set it as the operating region; otherwise, use the session region directly
    // as the operating region.
    this.operatingRegion = getSessionManager().getEnableLocalCache() ? createOrRetrieveLocalRegion() : this.sessionRegion;

    // Create or retrieve the statistics
    createStatistics();
  }

  @Override
  public String getDefaultRegionAttributesId() {
    return DEFAULT_REGION_ATTRIBUTES_ID;
  }

  @Override
  public boolean getDefaultEnableLocalCache() {
    return DEFAULT_ENABLE_LOCAL_CACHE;
  }

  @Override
  public void touchSessions(Set<String> sessionIds) {
    // Get the region attributes id to determine the region type. This is
    // problematic since the region attributes id doesn't really define the
    // region type. This should look at the actual session region.
    String regionAttributesID = getSessionManager().getRegionAttributesId().toLowerCase();

    // Invoke the appropriate function depending on the type of region
    ResultCollector collector = null;
    if (regionAttributesID.startsWith("partition")) {
      // Execute the partitioned touch function on the primary server(s)
      Execution execution = FunctionService.onRegion(getSessionRegion()).withFilter(sessionIds);
      collector = execution.execute(TouchPartitionedRegionEntriesFunction.ID, true, false, true);
    } else {
      // Execute the member touch function on all the server(s)
      Execution execution = FunctionService.onMembers(getCache().getDistributedSystem())
          .withArgs(new Object[]{this.sessionRegion.getFullPath(), sessionIds});
      collector = execution.execute(TouchReplicatedRegionEntriesFunction.ID, true, false, false);
    }

    // Get the result
    try {
      collector.getResult();
    } catch (Exception e) {
      // If an exception occurs in the function, log it.
      getSessionManager().getLogger().warn("Caught unexpected exception:", e);
    }
  }

  @Override
  public boolean isPeerToPeer() {
    return true;
  }

  @Override
  public boolean isClientServer() {
    return false;
  }

  @Override
  public Set<String> keySet() {
    return getSessionRegion().keySet();
  }

  @Override
  public int size() {
    return getSessionRegion().size();
  }

  @Override
  public GemFireCache getCache() {
    return this.cache;
  }

  /**
   * For peer-to-peer the backing cache *is* what's embedded in tomcat so it's always available
   *
   * @return boolean indicating whether a backing cache is available
   */
  @Override
  public boolean isBackingCacheAvailable() {
    return true;
  }

  private void registerFunctions() {
    // Register the touch partitioned region entries function if it is not already registered
    if (!FunctionService.isRegistered(TouchPartitionedRegionEntriesFunction.ID)) {
      FunctionService.registerFunction(new TouchPartitionedRegionEntriesFunction());
    }

    // Register the touch replicated region entries function if it is not already registered
    if (!FunctionService.isRegistered(TouchReplicatedRegionEntriesFunction.ID)) {
      FunctionService.registerFunction(new TouchReplicatedRegionEntriesFunction());
    }
  }

  @SuppressWarnings("unchecked")
  protected void createOrRetrieveRegion() {
    // Create the RegionConfiguration
    RegionConfiguration configuration = createRegionConfiguration();
    configuration.setSessionExpirationCacheListener(true);

    // Attempt to retrieve the region
    // If it already exists, validate it
    // If it doesn't already exist, create it
    Region region = this.cache.getRegion(getSessionManager().getRegionName());
    if (region == null) {
      // Create the region
      region = RegionHelper.createRegion((Cache) getCache(), configuration);
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Created new session region: " + region);
      }
    } else {
      // Validate the existing region
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Retrieved existing session region: " + region);
      }
      RegionHelper.validateRegion((Cache) getCache(), configuration, region);
    }

    // Set the session region
    this.sessionRegion = region;
  }

  private Region<String, HttpSession> createOrRetrieveLocalRegion() {
    // Attempt to retrieve the fronting region
    String frontingRegionName = this.sessionRegion.getName() + "_local";
    Region<String, HttpSession> frontingRegion = this.cache.getRegion(frontingRegionName);
    if (frontingRegion == null) {
      // Create the region factory
      RegionFactory<String, HttpSession> factory = this.cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);

      // Add the cache loader and writer
      factory.setCacheLoader(new LocalSessionCacheLoader(this.sessionRegion));
      factory.setCacheWriter(new LocalSessionCacheWriter(this.sessionRegion));

      // Set the expiration time, action and listener if necessary
      int maxInactiveInterval = getSessionManager().getMaxInactiveInterval();
      if (maxInactiveInterval != RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL) {
        factory.setStatisticsEnabled(true);
        factory.setCustomEntryIdleTimeout(new SessionCustomExpiry());
        factory.addCacheListener(new SessionExpirationCacheListener());
      }

      // Create the region
      frontingRegion = factory.create(frontingRegionName);
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Created new local session region: " + frontingRegion);
      }
    } else {
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Retrieved existing local session region: " + frontingRegion);
      }
    }
    return frontingRegion;
  }
}  
