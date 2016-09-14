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
package org.apache.geode.internal.admin;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.admin.remote.ClientHealthStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.management.internal.cli.CliUtil;

/**
 * This class publishes the client statistics using the admin region.
 * 
 */
public class ClientStatsManager {

  /**
   * Last cache that was initialized
   * 
   * @guarded.By ClientStatsManager.class
   */
  static GemFireCacheImpl lastInitializedCache = null;
  
  /**
   * @guarded.By ClientStatsManager.class
   */
  private static Statistics cachePerfStats = null;
  
  /**
   * @guarded.By ClientStatsManager.class
   */
  private static Statistics vmStats = null;
  
  /**
   * This method publishes the client stats using the admin region.
   * 
   * @param pool
   *                Connection pool which may be used for admin region.
   */
  public static synchronized void publishClientStats(PoolImpl pool) {
    GemFireCacheImpl currentCache = GemFireCacheImpl.getInstance();
    if (!initializeStatistics(currentCache)) {
      return; // handles null case too
    }
    LogWriterI18n logger = currentCache.getLoggerI18n();
    if (logger.fineEnabled())
      logger.fine("Entering ClientStatsManager#publishClientStats...");

    ClientHealthStats stats = getClientHealthStats(currentCache, pool);

    try {
      InternalDistributedSystem ds = (InternalDistributedSystem)
          currentCache.getDistributedSystem();
      ServerRegionProxy regionProxy = new ServerRegionProxy(ClientHealthMonitoringRegion.ADMIN_REGION_NAME, pool);
      
      EventID eventId = new EventID(ds);
      @Released EntryEventImpl event = new EntryEventImpl((Object)null);
      try {
      event.setEventId(eventId);
      regionProxy.putForMetaRegion(ds.getMemberId(), stats, null, event, null, true);
      } finally {
        event.release();
      }
    }
    catch (DistributedSystemDisconnectedException e) {
      throw e;
    }
    catch (CacheWriterException cwx) {
      pool.getCancelCriterion().checkCancelInProgress(cwx);
      currentCache.getCancelCriterion().checkCancelInProgress(cwx);
      // TODO: Need to analyze these exception scenarios.
      logger.warning(LocalizedStrings.ClientStatsManager_FAILED_TO_SEND_CLIENT_HEALTH_STATS_TO_CACHESERVER, cwx);
    }
    catch (Exception e) {
      pool.getCancelCriterion().checkCancelInProgress(e);
      currentCache.getCancelCriterion().checkCancelInProgress(e);
      logger.info(LocalizedStrings.ClientStatsManager_FAILED_TO_PUBLISH_CLIENT_STATISTICS, e);
    }

    if (logger.fineEnabled())
      logger.fine("Exiting ClientStatsManager#publishClientStats.");

  }
  
  public static void cleanupForTests() {
    lastInitializedCache = null;
  }

  /**
   * This method initializes the client statistics to be queried.
   * 
   * @return true if statistics correctly initialized
   * @guarded.By ClientStatsManager.class
   */
  private static boolean initializeStatistics(GemFireCacheImpl currentCache) {
   
    if (currentCache == null) {
      return false;
    }
    LogWriterI18n logger = currentCache.getLoggerI18n();
    InternalDistributedSystem ds = (InternalDistributedSystem)currentCache.getDistributedSystem();
    if (currentCache.isClosed()) {
      return false;
    }
    
    boolean restart = lastInitializedCache != currentCache;
    lastInitializedCache = currentCache;
    
    if(restart) {
      if(logger.infoEnabled()) {
        logger.info(LocalizedStrings.ClientStatsManager_CLIENTSTATSMANAGER_INTIALIZING_THE_STATISTICS);
      }
      cachePerfStats = null;
      vmStats = null;
    }
      
    if(cachePerfStats == null) {
      StatisticsType type = ds.findType("CachePerfStats");
      if(type != null) {
        Statistics[] statistics = ds.findStatisticsByType(type);
        if(statistics != null && statistics.length > 0) {
          cachePerfStats = statistics[0];
        }
      }
    }
      
    if(vmStats == null) {
      StatisticsType type = ds.findType("VMStats");
      if(type != null) {
        Statistics[] statistics = ds.findStatisticsByType(type);
        if(statistics != null && statistics.length > 0) {
          vmStats = statistics[0];
        }
      }
    }
    
    // Validate that cache has changed before logging the warning, thus logging it once per cache
    if(cachePerfStats == null && restart) {
     logger.warning(LocalizedStrings.ClientStatsManager_CLIENTSTATSMANAGER_0_ARE_NOT_AVAILABLE, "CachePerfStats");
    }
      
    // Validate that cache has changed before logging the warning, thus logging it once per cache
    if(vmStats == null && restart) {
     logger.warning(LocalizedStrings.ClientStatsManager_CLIENTSTATSMANAGER_0_ARE_NOT_AVAILABLE, "VMStats");
    }
   

    return true;
  }

  /**
   * This method queries the client stats & prepares the client health stats
   * object to be published to the server.
   * 
   * @return the client health stats object to be published to the server.
   */
  private static ClientHealthStats getClientHealthStats(GemFireCacheImpl currentCache, PoolImpl pool) {
    if (currentCache == null) {
      return null;
    }
    ClientHealthStats stats = new ClientHealthStats();
    LogWriterI18n logger = currentCache.getLoggerI18n();
    {
      int gets = -1;
      int puts = -1;
      int misses = -1;
      long processCpuTime = -1;
      int threads = -1;
      int cacheListenerCalls = -1;
      int cpus = -1;
      String poolName = "";

      if(cachePerfStats != null) {
        gets = cachePerfStats.getInt("gets");
        puts = cachePerfStats.getInt("puts");
        misses = cachePerfStats.getInt("misses");
        cacheListenerCalls = cachePerfStats.getInt("cacheListenerCallsCompleted");
      }

      if(vmStats != null) {
        processCpuTime = vmStats.getLong("processCpuTime");
        threads =vmStats.getInt("threads");
        cpus = vmStats.getInt("cpus");
      }

      stats.setNumOfGets(gets);
      stats.setNumOfPuts(puts);
      stats.setNumOfMisses(misses);
      stats.setNumOfCacheListenerCalls(cacheListenerCalls);
      stats.setProcessCpuTime(processCpuTime);
      stats.setNumOfThreads(threads);
      stats.setCpus(cpus);

      poolName = pool.getName();
      try {
        Map<String, String> newPoolStats = stats.getPoolStats();
        String poolStatsStr = "MinConnections=" + pool.getMinConnections() + ";MaxConnections=" + pool.getMaxConnections() 
            + ";Redudancy=" + pool.getSubscriptionRedundancy() + ";CQS=" + pool.getQueryService().getCqs().length  ;
        logger.info(LocalizedStrings.DEBUG,"ClientHealthStats for poolname " + poolName  + " poolStatsStr=" + poolStatsStr);

       newPoolStats.put(poolName, poolStatsStr);

        // consider old stats
       Region clientHealthMonitoringRegion = ClientHealthMonitoringRegion.getInstance(currentCache);

       if (clientHealthMonitoringRegion != null) {
         InternalDistributedSystem ds = (InternalDistributedSystem) currentCache.getDistributedSystem();            
         ClientHealthStats oldStats = (ClientHealthStats) clientHealthMonitoringRegion.get(ds.getMemberId());
         logger.info(LocalizedStrings.DEBUG,"getClientHealthStats got oldStats  "  + oldStats);
         if (oldStats != null) {              
           Map<String, String> oldPoolStats = oldStats.getPoolStats();
           logger.info(LocalizedStrings.DEBUG,"getClientHealthStats got oldPoolStats  "  + oldPoolStats);
           if (oldPoolStats != null) {
             Iterator<Entry<String, String>> it = oldPoolStats.entrySet().iterator();
             while (it.hasNext()) {
               Entry<String, String> entry = it.next();                  
               if (!poolName.equals(entry.getKey())) {
                 stats.getPoolStats().put(entry.getKey(), entry.getValue());
               }
             }
           }
         }
       } 

     } catch (Exception e) {
         logger.fine("Exception in getting pool stats in  getClientHealthStats " + CliUtil.stackTraceAsString(e));
     }      
    }
    stats.setUpdateTime(new Date());
    return stats;
  }
}
