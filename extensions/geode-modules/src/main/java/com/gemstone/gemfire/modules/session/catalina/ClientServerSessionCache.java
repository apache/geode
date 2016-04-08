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

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.modules.session.catalina.callback.SessionExpirationCacheListener;
import com.gemstone.gemfire.modules.util.BootstrappingFunction;
import com.gemstone.gemfire.modules.util.CreateRegionFunction;
import com.gemstone.gemfire.modules.util.RegionConfiguration;
import com.gemstone.gemfire.modules.util.RegionSizeFunction;
import com.gemstone.gemfire.modules.util.RegionStatus;
import com.gemstone.gemfire.modules.util.SessionCustomExpiry;
import com.gemstone.gemfire.modules.util.TouchPartitionedRegionEntriesFunction;
import com.gemstone.gemfire.modules.util.TouchReplicatedRegionEntriesFunction;

import javax.servlet.http.HttpSession;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ClientServerSessionCache extends AbstractSessionCache {

  private ClientCache cache;

  protected static final String DEFAULT_REGION_ATTRIBUTES_ID = RegionShortcut.PARTITION_REDUNDANT.toString();

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
      Execution execution = FunctionService.onRegion(getSessionRegion()).withFilter(sessionIds);
      try {
        ResultCollector collector = execution.execute(TouchPartitionedRegionEntriesFunction.ID, true, false, true);
        collector.getResult();
      } catch (Exception e) {
        // If an exception occurs in the function, log it.
        getSessionManager().getLogger().warn("Caught unexpected exception:", e);
      }
    } else {
      // Execute the member touch function on all the server(s)
      Execution execution = FunctionService.onServers(getCache())
          .withArgs(new Object[]{this.sessionRegion.getFullPath(), sessionIds});
      try {
        ResultCollector collector = execution.execute(TouchReplicatedRegionEntriesFunction.ID, true, false, false);
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
    // Add a single dummy key to force the function to go to one server
    Set<String> filters = new HashSet<String>();
    filters.add("test-key");

    // Execute the function on the session region
    Execution execution = FunctionService.onRegion(getSessionRegion()).withFilter(filters);
    ResultCollector collector = execution.execute(RegionSizeFunction.ID, true, true, true);
    List<Integer> result = (List<Integer>) collector.getResult();

    // Return the first (and only) element
    return result.get(0);
  }

  @Override
  public boolean isBackingCacheAvailable() {
    if (getSessionManager().isCommitValveFailfastEnabled()) {
      PoolImpl pool = (PoolImpl) PoolManager.find(getOperatingRegionName());
      return pool.isPrimaryUpdaterAlive();
    }
    return true;
  }

  public GemFireCache getCache() {
    return this.cache;
  }

  private void bootstrapServers() {
    Execution execution = FunctionService.onServers(this.cache);
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
      this.sessionRegion = createLocalSessionRegion();
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Created session region: " + this.sessionRegion);
      }
    } else {
      if (getSessionManager().getLogger().isDebugEnabled()) {
        getSessionManager().getLogger().debug("Retrieved session region: " + this.sessionRegion);
      }
    }
  }

  private void createSessionRegionOnServers() {
    // Create the RegionConfiguration
    RegionConfiguration configuration = createRegionConfiguration();

    // Send it to the server tier
    Execution execution = FunctionService.onServer(this.cache).withArgs(configuration);
    ResultCollector collector = execution.execute(CreateRegionFunction.ID);

    // Verify the region was successfully created on the servers
    List<RegionStatus> results = (List<RegionStatus>) collector.getResult();
    for (RegionStatus status : results) {
      if (status == RegionStatus.INVALID) {
        StringBuilder builder = new StringBuilder();
        builder.append("An exception occurred on the server while attempting to create or validate region named ")
            .append(getSessionManager().getRegionName())
            .append(". See the server log for additional details.");
        throw new IllegalStateException(builder.toString());
      }
    }
  }

  private Region<String, HttpSession> createLocalSessionRegion() {
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
    Region region = factory.create(getSessionManager().getRegionName());

    /*
     * If we're using an empty client region, we register interest so that
     * expired sessions are destroyed correctly.
     */
    if (!getSessionManager().getEnableLocalCache()) {
      region.registerInterest("ALL_KEYS", InterestResultPolicy.KEYS);
    }

    return region;
  }
}
