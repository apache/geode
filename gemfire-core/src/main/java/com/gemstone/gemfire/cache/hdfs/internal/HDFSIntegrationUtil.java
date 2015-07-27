/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Contains utility functions
 *
 * @author Hemant Bhanawat
 *
 */
public class HDFSIntegrationUtil {

  public static <K, V> AsyncEventQueue createDefaultAsyncQueueForHDFS(Cache cache, boolean writeOnly, String regionPath) {
    return createAsyncQueueForHDFS(cache, regionPath, writeOnly, null);
  }

  private static AsyncEventQueue createAsyncQueueForHDFS(Cache cache, String regionPath, boolean writeOnly,
      HDFSStore configView) {
    LogWriterI18n logger = cache.getLoggerI18n();
    String defaultAsyncQueueName = HDFSStoreFactoryImpl.getEventQueueName(regionPath);

    if (configView == null) {
      configView = new HDFSStoreFactoryImpl(cache).getConfigView();
    }
    

    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();
    factory.setBatchSize(configView.getBatchSize());
    factory.setPersistent(configView.getBufferPersistent());
    factory.setDiskStoreName(configView.getDiskStoreName());
    factory.setMaximumQueueMemory(configView.getMaxMemory());
    factory.setBatchTimeInterval(configView.getBatchInterval());
    factory.setDiskSynchronous(configView.getSynchronousDiskWrite());
    factory.setDispatcherThreads(configView.getDispatcherThreads());
    factory.setParallel(true);
    factory.addGatewayEventFilter(new HDFSEventQueueFilter(logger));
    ((AsyncEventQueueFactoryImpl) factory).setBucketSorted(!writeOnly);
    ((AsyncEventQueueFactoryImpl) factory).setIsHDFSQueue(true);

    AsyncEventQueue asyncQ = null;

    if (!writeOnly)
      asyncQ = factory.create(defaultAsyncQueueName, new HDFSEventListener(cache.getLoggerI18n()));
    else
      asyncQ = factory.create(defaultAsyncQueueName, new HDFSWriteOnlyStoreEventListener(cache.getLoggerI18n()));

    logger.fine("HDFS: async queue created for HDFS. Id: " + asyncQ.getId() + ". Disk store: "
        + asyncQ.getDiskStoreName() + ". Batch size: " + asyncQ.getBatchSize() + ". bucket sorted:  " + !writeOnly);
    return asyncQ;

  }

  public static void createAndAddAsyncQueue(String regionPath, RegionAttributes regionAttributes, Cache cache) {
    if (!regionAttributes.getDataPolicy().withHDFS()) {
      return;
    }

    String leaderRegionPath = getLeaderRegionPath(regionPath, regionAttributes, cache);

    String defaultAsyncQueueName = HDFSStoreFactoryImpl.getEventQueueName(leaderRegionPath);
    if (cache.getAsyncEventQueue(defaultAsyncQueueName) == null) {
      if (regionAttributes.getHDFSStoreName() != null && regionAttributes.getPartitionAttributes() != null
          && !(regionAttributes.getPartitionAttributes().getLocalMaxMemory() == 0)) {
        HDFSStore store = ((GemFireCacheImpl) cache).findHDFSStore(regionAttributes.getHDFSStoreName());
        if (store == null) {
          throw new IllegalStateException(
              LocalizedStrings.HOPLOG_HDFS_STORE_NOT_FOUND.toLocalizedString(regionAttributes.getHDFSStoreName()));
        }
        HDFSIntegrationUtil
            .createAsyncQueueForHDFS(cache, leaderRegionPath, regionAttributes.getHDFSWriteOnly(), store);
      }
    }
  }

  private static String getLeaderRegionPath(String regionPath, RegionAttributes regionAttributes, Cache cache) {
    String colocated;
    while (regionAttributes.getPartitionAttributes() != null
        && (colocated = regionAttributes.getPartitionAttributes().getColocatedWith()) != null) {
      // Do not waitOnInitialization() for PR
      GemFireCacheImpl gfc = (GemFireCacheImpl) cache;
      Region colocatedRegion = gfc.getPartitionedRegion(colocated, false);
      if (colocatedRegion == null) {
        Assert.fail("Could not find parent region " + colocated + " for " + regionPath);
      }
      regionAttributes = colocatedRegion.getAttributes();
      regionPath = colocatedRegion.getFullPath();
    }
    return regionPath;
  }

}
