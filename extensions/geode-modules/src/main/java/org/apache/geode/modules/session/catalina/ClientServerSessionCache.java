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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.modules.util.BootstrappingFunction;
import org.apache.geode.modules.util.CreateRegionFunction;
import org.apache.geode.modules.util.RegionConfiguration;
import org.apache.geode.modules.util.RegionStatus;
import org.apache.geode.modules.util.SessionCustomExpiry;
import org.apache.geode.modules.util.TouchPartitionedRegionEntriesFunction;
import org.apache.geode.modules.util.TouchReplicatedRegionEntriesFunction;

public class ClientServerSessionCache extends AbstractSessionCache {

  private ClientCache cache;

  protected static final String DEFAULT_REGION_ATTRIBUTES_ID =
      RegionShortcut.PARTITION_REDUNDANT.toString();

  protected static final boolean DEFAULT_ENABLE_LOCAL_CACHE = true;

  public ClientServerSessionCache(SessionManager sessionManager, ClientCache cache) {
    super(sessionManager);
    this.cache = cache;
  }

  @Override
  public void initialize() {
    // Bootstrap the servers
    bootstrapServers();

    // Create or retrieve the region
    try {
      createOrRetrieveRegion();
    } catch (Exception ex) {
      sessionManager.getLogger().fatal("Unable to create or retrieve region", ex);
      throw new IllegalStateException(ex);
    }

    // Set the session region directly as the operating region since there is no difference
    // between the local cache region and the session region.
    this.operatingRegion = this.sessionRegion;

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
    // region type. Currently there is no way to know the type of region created
    // on the server. Maybe the CreateRegionFunction should return it.
    String regionAttributesID = getSessionManager().getRegionAttributesId().toLowerCase();

    // Invoke the appropriate function depending on the type of region
    if (regionAttributesID.startsWith("partition")) {
      // Execute the partitioned touch function on the primary server(s)
      Execution execution = getExecutionForFunctionOnRegionWithFilter(sessionIds);
      try {
        ResultCollector collector = execution.execute(TouchPartitionedRegionEntriesFunction.ID);
        collector.getResult();
      } catch (Exception e) {
        // If an exception occurs in the function, log it.
        getSessionManager().getLogger().warn("Caught unexpected exception:", e);
      }
    } else {
      // Execute the member touch function on all the server(s)
      Object[] arguments = new Object[] {this.sessionRegion.getFullPath(), sessionIds};
      Execution execution = getExecutionForFunctionOnServersWithArguments(arguments);
      try {
        ResultCollector collector = execution.execute(TouchReplicatedRegionEntriesFunction.ID);
        collector.getResult();
      } catch (Exception e) {
        // If an exception occurs in the function, log it.
        getSessionManager().getLogger().warn("Caught unexpected exception:", e);
      }
    }
  }

  @Override
  public boolean isPeerToPeer() {
    return false;
  }

  @Override
  public boolean isClientServer() {
    return true;
  }

  @Override
  public Set<String> keySet() {
    return getSessionRegion().keySetOnServer();
  }

  @Override
  public int size() {
    return getSessionRegion().sizeOnServer();
  }

  @Override
  public boolean isBackingCacheAvailable() {
    if (getSessionManager().isCommitValveFailfastEnabled()) {
      PoolImpl pool = findPoolInPoolManager();
      return pool.isPrimaryUpdaterAlive();
    }
    return true;
  }

  @Override
  public GemFireCache getCache() {
    return this.cache;
  }

  private void bootstrapServers() {
    Execution execution = getExecutionForFunctionOnServers();
    ResultCollector collector = execution.execute(new BootstrappingFunction());
    // Get the result. Nothing is being done with it.
    try {
      collector.getResult();
    } catch (Exception e) {
      // If an exception occurs in the function, log it.
      getSessionManager().getLogger().warn("Caught unexpected exception:", e);
    }
  }

  protected void createOrRetrieveRegion() {
    // Retrieve the local session region
    this.sessionRegion = this.cache.getRegion(getSessionManager().getRegionName());

    // If necessary, create the regions on the server and client
    if (this.sessionRegion == null) {
      // Create the PR on the servers
      createSessionRegionOnServers();

      // Create the region on the client
      this.sessionRegion = createLocalSessionRegionWithRegisterInterest();
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Created session region: " + this.sessionRegion);
      }
    } else {
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Retrieved session region: " + this.sessionRegion);
      }

      // Check that we have our expiration listener attached
      if (!regionHasExpirationListenerAttached(sessionRegion)) {
        sessionRegion.getAttributesMutator().addCacheListener(new SessionExpirationCacheListener());
      }

      if (sessionRegion.getAttributes().getDataPolicy() != DataPolicy.EMPTY) {
        sessionRegion.registerInterestForAllKeys(InterestResultPolicy.KEYS);
      }
    }
  }

  private boolean regionHasExpirationListenerAttached(Region<?, ?> region) {
    return Arrays.stream(region.getAttributes().getCacheListeners())
        .anyMatch(x -> x instanceof SessionExpirationCacheListener);
  }

  void createSessionRegionOnServers() {
    // Create the RegionConfiguration
    RegionConfiguration configuration = createRegionConfiguration();

    // Send it to the server tier
    Execution execution = getExecutionForFunctionOnServerWithRegionConfiguration(configuration);
    ResultCollector collector = execution.execute(CreateRegionFunction.ID);

    // Verify the region was successfully created on the servers
    List<RegionStatus> results = (List<RegionStatus>) collector.getResult();
    for (RegionStatus status : results) {
      if (status == RegionStatus.INVALID) {
        StringBuilder builder = new StringBuilder();
        builder
            .append(
                "An exception occurred on the server while attempting to create or validate region named ")
            .append(getSessionManager().getRegionName())
            .append(". See the server log for additional details.");
        throw new IllegalStateException(builder.toString());
      }
    }
  }

  Region<String, HttpSession> createLocalSessionRegionWithRegisterInterest() {
    Region<String, HttpSession> region = createLocalSessionRegion();

    // register interest are needed for caching proxy client:
    // to get updates from server if local cache is enabled;
    if (region.getAttributes().getDataPolicy() != DataPolicy.EMPTY) {
      region.registerInterestForAllKeys(InterestResultPolicy.KEYS);
    }

    return region;
  }

  Region<String, HttpSession> createLocalSessionRegion() {
    ClientRegionFactory<String, HttpSession> factory = null;
    if (getSessionManager().getEnableLocalCache()) {
      // Create the region factory with caching and heap LRU enabled
      factory = this.cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY_HEAP_LRU);

      // Set the expiration time, action and listener if necessary
      int maxInactiveInterval = getSessionManager().getMaxInactiveInterval();
      if (maxInactiveInterval != RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL) {
        factory.setStatisticsEnabled(true);
        factory.setCustomEntryIdleTimeout(new SessionCustomExpiry());
        factory.addCacheListener(new SessionExpirationCacheListener());
      }
    } else {
      // Create the region factory without caching enabled
      factory = this.cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      factory.addCacheListener(new SessionExpirationCacheListener());
    }

    // Create the region
    return factory.create(getSessionManager().getRegionName());
  }

  // Helper methods added to improve unit testing of class
  Execution getExecutionForFunctionOnServers() {
    return getExecutionForFunctionOnServersWithArguments(null);
  }

  Execution getExecutionForFunctionOnServersWithArguments(Object[] arguments) {
    if (arguments != null && arguments.length > 0) {
      return FunctionService.onServers(getCache()).setArguments(arguments);
    } else {
      return FunctionService.onServers(getCache());
    }
  }

  Execution getExecutionForFunctionOnServerWithRegionConfiguration(RegionConfiguration arguments) {
    if (arguments != null) {
      return FunctionService.onServer(getCache()).setArguments(arguments);
    } else {
      return FunctionService.onServer(getCache());
    }
  }

  Execution getExecutionForFunctionOnRegionWithFilter(Set<?> filter) {
    if (filter != null && filter.size() > 0) {
      return FunctionService.onRegion(getSessionRegion()).withFilter(filter);
    } else {
      return FunctionService.onRegion(getSessionRegion());
    }
  }

  PoolImpl findPoolInPoolManager() {
    return (PoolImpl) PoolManager.find(getOperatingRegionName());
  }
}
