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
package org.apache.geode.internal.admin;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.admin.remote.ClientHealthStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This is an admin (meta) region used by the client health monitoring service to publish the client
 * health details to the cache-server.
 */
public class ClientHealthMonitoringRegion {
  private static final Logger logger = LogService.getLogger();

  public static final String ADMIN_REGION_NAME = "__ADMIN_CLIENT_HEALTH_MONITORING__";

  static final int ADMIN_REGION_EXPIRY_INTERVAL = 20;

  /**
   * Instance for current cache
   * <p>
   * GuardedBy ClientHealthMonitoringRegion.class
   */
  @MakeNotStatic
  private static Region currentInstance;

  /**
   * This is an accessor method used to get the reference of this region. If this region is not yet
   * initialized, then it attempts to create it.
   *
   * @param cache the Cache we are currently using
   * @return ClientHealthMonitoringRegion reference.
   */
  public static synchronized Region getInstance(InternalCache cache) {
    if (currentInstance != null && currentInstance.getCache() == cache && !cache.isClosed()) {
      return currentInstance;
    }
    if (cache == null || cache.isClosed()) {
      return null; // give up
    }
    initialize(cache);
    return currentInstance;
  }

  /**
   * This method creates the client health monitoring region.
   * <p>
   * GuardedBy ClientHealthMonitoringRegion.class
   *
   * @param cache The current GemFire Cache
   */
  private static void initialize(InternalCache cache) {
    try {
      InternalRegionFactory factory = cache.createInternalRegionFactory(RegionShortcut.LOCAL);
      factory.setEntryTimeToLive(
          new ExpirationAttributes(ADMIN_REGION_EXPIRY_INTERVAL, ExpirationAction.DESTROY));
      if (logger.isDebugEnabled()) {
        logger.debug("ClientHealthMonitoringRegion, setting TTL for entry....");
      }
      factory.addCacheListener(prepareCacheListener());
      factory.setValueConstraint(ClientHealthStats.class);
      factory.setStatisticsEnabled(true);
      factory.setIsUsedForMetaRegion(true).setIsUsedForPartitionedRegionAdmin(false);
      currentInstance = factory.create(ADMIN_REGION_NAME);
    } catch (Exception ex) {
      logger.error("Error while creating an admin region", ex);
    }
  }

  /**
   * This method prepares a CacheListener, responsible for the cleanup of reference of admin region,
   * upon the cache closure.
   *
   * @return CacheListener.
   */
  private static CacheListener prepareCacheListener() {
    return new CacheListenerAdapter() {
      @Override
      public void close() {
        synchronized (ClientHealthMonitoringRegion.class) {
          ClientHealthMonitoringRegion.currentInstance = null;
        }
      }
    };
  }
}
