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
package org.apache.geode.modules.session.catalina;

import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.modules.session.catalina.callback.LocalSessionCacheLoader;
import org.apache.geode.modules.session.catalina.callback.LocalSessionCacheWriter;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.modules.util.RegionConfiguration;
import org.apache.geode.modules.util.RegionHelper;
import org.apache.geode.modules.util.SessionCustomExpiry;
import org.apache.geode.modules.util.TouchPartitionedRegionEntriesFunction;
import org.apache.geode.modules.util.TouchReplicatedRegionEntriesFunction;

public class PeerToPeerSessionCache extends AbstractSessionCache {

  private Cache cache;

  protected static final String DEFAULT_REGION_ATTRIBUTES_ID = RegionShortcut.REPLICATE.toString();

  protected static final boolean DEFAULT_ENABLE_LOCAL_CACHE = false;

  public PeerToPeerSessionCache(SessionManager sessionManager, Cache cache) {
    super(sessionManager);
    this.cache = cache;
    addReconnectListener();
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
    this.operatingRegion = getSessionManager().getEnableLocalCache() ? createOrRetrieveLocalRegion()
        : this.sessionRegion;

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
      Execution execution = getExecutionForFunctionOnRegionWithFilter(sessionIds);
      collector = execution.execute(TouchPartitionedRegionEntriesFunction.ID);
    } else {
      // Execute the member touch function on all the server(s)
      Execution execution = getExecutionForFunctionOnMembersWithArguments(
          new Object[] {this.sessionRegion.getFullPath(), sessionIds});
      collector = execution.execute(TouchReplicatedRegionEntriesFunction.ID);
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
    if (!isFunctionRegistered(TouchPartitionedRegionEntriesFunction.ID)) {
      registerFunctionWithFunctionService(new TouchPartitionedRegionEntriesFunction());
    }

    // Register the touch replicated region entries function if it is not already registered
    if (!isFunctionRegistered(TouchReplicatedRegionEntriesFunction.ID)) {
      registerFunctionWithFunctionService(new TouchReplicatedRegionEntriesFunction());
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
      region = createRegionUsingHelper(configuration);
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Created new session region: " + region);
      }
    } else {
      // Validate the existing region
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Retrieved existing session region: " + region);
      }
      validateRegionUsingRegionhelper(configuration, region);
    }

    // Set the session region
    this.sessionRegion = region;
  }

  void validateRegionUsingRegionhelper(RegionConfiguration configuration, Region region) {
    RegionHelper.validateRegion((Cache) getCache(), configuration, region);
  }

  Region createRegionUsingHelper(RegionConfiguration configuration) {
    return RegionHelper.createRegion((Cache) getCache(), configuration);
  }

  private Region<String, HttpSession> createOrRetrieveLocalRegion() {
    // Attempt to retrieve the fronting region
    String frontingRegionName = this.sessionRegion.getName() + "_local";
    Region<String, HttpSession> frontingRegion = this.cache.getRegion(frontingRegionName);
    if (frontingRegion == null) {
      // Create the region factory
      RegionFactory<String, HttpSession> factory =
          this.cache.createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);

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
        getSessionManager().getLogger()
            .debug("Created new local session region: " + frontingRegion);
      }
    } else {
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger()
            .debug("Retrieved existing local session region: " + frontingRegion);
      }
    }
    return frontingRegion;
  }

  // Helper methods added to improve unit testing of class
  void registerFunctionWithFunctionService(Function function) {
    FunctionService.registerFunction(function);
  }

  boolean isFunctionRegistered(String id) {
    return FunctionService.isRegistered(id);
  }

  Execution getExecutionForFunctionOnRegionWithFilter(Set<String> sessionIds) {
    return FunctionService.onRegion(getSessionRegion()).withFilter(sessionIds);
  }

  Execution getExecutionForFunctionOnMembersWithArguments(Object[] arguments) {
    return FunctionService.onMembers().setArguments(arguments);
  }

  private void addReconnectListener() {
    InternalDistributedSystem.addReconnectListener(
        new InternalDistributedSystem.ReconnectListener() {
          @Override
          public void onReconnect(InternalDistributedSystem oldSystem,
              InternalDistributedSystem newSystem) {
            reinitialize(newSystem.getCache());
          }
        });
  }

  private void reinitialize(InternalCache reconnectedCache) {
    this.cache = reconnectedCache;
    initialize();
  }
}
