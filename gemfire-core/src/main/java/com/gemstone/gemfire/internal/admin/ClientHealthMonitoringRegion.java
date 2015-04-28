/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This is an admin (meta) region used by the client health monitoring service
 * to publish the client health details to the cache-server.
 * 
 * @author hkhanna
 * @author Hrishi
 */

public class ClientHealthMonitoringRegion {
  public final static String ADMIN_REGION_NAME = "__ADMIN_CLIENT_HEALTH_MONITORING__";

  public final static int ADMIN_REGION_EXPIRY_INTERVAL = 20;

  /**
   * Instance for current cache
   * 
   * @guarded.By ClientHealthMonitoringRegion.class
   */
  static Region currentInstance;

  /**
   * This is an accessor method used to get the reference of this region. If
   * this region is not yet initialized, then it attempts to create it.
   * 
   * @param c the Cache we are currently using
   * @return ClientHealthMonitoringRegion reference.
   */
  public static synchronized Region getInstance(GemFireCacheImpl c) {
    if (currentInstance != null && currentInstance.getCache() == c 
        && !c.isClosed()) {
      return currentInstance;
    }
    if (c == null || c.isClosed()) {
      return null; // give up
    }
    initialize(c);
    return currentInstance;
  }

  /**
   * This method creates the client health monitoring region.
   * 
   * @param cache
   *                The current GemFire Cache
   * @guarded.By ClientHealthMonitoringRegion.class
   */
  private static void initialize(GemFireCacheImpl cache) {
    try {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      factory.setEntryTimeToLive(new ExpirationAttributes(
          ADMIN_REGION_EXPIRY_INTERVAL, ExpirationAction.DESTROY));
      cache.getLogger().fine("ClientHealthMonitoringRegion, setting TTL for entry....");
      factory.addCacheListener(prepareCacheListener());
      factory.setStatisticsEnabled(true);
      RegionAttributes regionAttrs = factory.create();

      InternalRegionArguments internalArgs = new InternalRegionArguments();
      internalArgs.setIsUsedForMetaRegion(true);
      internalArgs.setIsUsedForPartitionedRegionAdmin(false);

      currentInstance = cache.createVMRegion(ADMIN_REGION_NAME, regionAttrs,
          internalArgs);
    }
    catch (Exception ex) {
      cache.getLoggerI18n().error(LocalizedStrings.
        ClientHealthMonitoringRegion_ERROR_WHILE_CREATING_AN_ADMIN_REGION, ex);
    }
  }

  /**
   * This method prepares a CacheListener, responsible for the cleanup of
   * reference of admin region, upon the cache closure.
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
