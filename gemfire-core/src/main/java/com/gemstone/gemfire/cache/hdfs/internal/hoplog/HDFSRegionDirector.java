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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

import org.apache.logging.log4j.Logger;

/**
 * Cache for hoplog organizers associated with buckets of a region. The director creates an
 * instance of organizer on first get request. It does not read HDFS in advance. Creation of
 * organizer depends on File system initialization that takes outside this class. This class also
 * provides utility methods to monitor usage and manage bucket sets.
 * 
 */
public class HDFSRegionDirector {
  /*
   * Maps each region name to its listener and store objects. This map must be populated before file
   * organizers of a bucket can be created
   */
  private final ConcurrentHashMap<String, HdfsRegionManager> regionManagerMap;
  
  /**
   * regions of this Gemfire cache are managed by this director. TODO this
   * should be final and be provided at the time of creation of this instance or
   * through a cache directory
   */
  private GemFireCache cache;
  
  // singleton instance
  private static HDFSRegionDirector instance;
  
  final ScheduledExecutorService janitor;
  private JanitorTask janitorTask;
  
  private static final Logger logger = LogService.getLogger();
  protected final static String logPrefix = "<" + "RegionDirector" + "> ";
  
  
  private HDFSRegionDirector() {
    regionManagerMap = new ConcurrentHashMap<String, HDFSRegionDirector.HdfsRegionManager>();
    janitor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "HDFSRegionJanitor");
        thread.setDaemon(true);
        return thread;
      }
    });
    
    long interval = Long.getLong(HoplogConfig.JANITOR_INTERVAL_SECS,
        HoplogConfig.JANITOR_INTERVAL_SECS_DEFAULT);
    
    janitorTask = new JanitorTask();
    janitor.scheduleWithFixedDelay(janitorTask, interval, interval,
        TimeUnit.SECONDS);
  }
  
  public synchronized static HDFSRegionDirector getInstance() {
    if (instance == null) {
      instance = new HDFSRegionDirector();
    }
    return instance;
  }
  
  public HDFSRegionDirector setCache(GemFireCache cache) {
    this.cache = cache;
    return this;
  }

  public GemFireCache getCache() {
    return this.cache;
  }
  /**
   * Caches listener, store object and list of organizers associated with the region associated with
   * a region. Subsequently, these objects will be used each time an organizer is created
   */
  public synchronized HdfsRegionManager manageRegion(LocalRegion region, String storeName,
      HoplogListener listener) {
    
    HdfsRegionManager manager = regionManagerMap.get(region.getFullPath());
    if (manager != null) {
      // this is an attempt to re-register a region. Assuming this was required
      // to modify listener or hdfs store impl associated with the region. Hence
      // will clear the region first.

      clear(region.getFullPath());
    }
    
    HDFSStoreImpl store = HDFSStoreDirector.getInstance().getHDFSStore(storeName);
    manager = new HdfsRegionManager(region, store, listener, getStatsFactory(), this);
    regionManagerMap.put(region.getFullPath(), manager);
    
    if (logger.isDebugEnabled()) {
      logger.debug("{}Now managing region " + region.getFullPath(), logPrefix);
    }
    
    return manager;
  }
  
  /**
   * Find the regions that are part of a particular HDFS store.
   */
  public Collection<String> getRegionsInStore(HDFSStore store) {
    TreeSet<String> regions = new TreeSet<String>();
    for(Map.Entry<String, HdfsRegionManager> entry : regionManagerMap.entrySet()) {
      if(entry.getValue().getStore().equals(store)) {
        regions.add(entry.getKey());
      }
    }
    return regions;
  }
  
  public int getBucketCount(String regionPath) {
    HdfsRegionManager manager = regionManagerMap.get(regionPath);
    if (manager == null) {
      throw new IllegalStateException("Region not initialized");
    }

    return manager.bucketOrganizerMap.size();
  }
  
  public void closeWritersForRegion(String regionPath, int minSizeForFileRollover) throws IOException {
    regionManagerMap.get(regionPath).closeWriters(minSizeForFileRollover);
  }
  /**
   * removes and closes all {@link HoplogOrganizer} of this region. This call is expected with
   * a PR disowns a region.
   */
  public synchronized void clear(String regionPath) {
    HdfsRegionManager manager = regionManagerMap.remove(regionPath);
    if (manager != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}Closing hoplog region manager for " + regionPath, logPrefix);
      }
      manager.close();
    }
  }

  /**
   * Closes all region managers, organizers and hoplogs. This method should be
   * called before closing the cache to gracefully release all resources
   */
  public static synchronized void reset() {
    if (instance == null) {
      // nothing to reset
      return;
    }
    
    instance.janitor.shutdownNow();
    
    for (String region : instance.regionManagerMap.keySet()) {
      instance.clear(region);
    }
    instance.cache = null;
    instance = null;
  }
  
  /**
   * Terminates current janitor task and schedules a new. The rate of the new
   * task is based on the value of system property at that time
   */
  public static synchronized void resetJanitor() {
    instance.janitorTask.terminate();
    instance.janitorTask = instance.new JanitorTask();
    long interval = Long.getLong(HoplogConfig.JANITOR_INTERVAL_SECS,
        HoplogConfig.JANITOR_INTERVAL_SECS_DEFAULT);
    instance.janitor.scheduleWithFixedDelay(instance.janitorTask, 0, interval,
        TimeUnit.SECONDS);
  }
  
  /**
   * @param regionPath name of region for which stats object is desired
   * @return {@link SortedOplogStatistics} instance associated with hdfs region
   *         name. Null if region is not managed by director
   */
  public synchronized SortedOplogStatistics getHdfsRegionStats(String regionPath) {
    HdfsRegionManager manager = regionManagerMap.get(regionPath);
    return manager == null ? null : manager.getHdfsStats();
  }
  
  private StatisticsFactory getStatsFactory() {
    return cache.getDistributedSystem();
  }

  /**
   * A helper class to manage region and its organizers
   */
  public static class HdfsRegionManager {
    // name and store configuration of the region whose buckets are managed by this director.
    private LocalRegion region;
    private HDFSStoreImpl store;
    private HoplogListener listener;
    private volatile boolean closed = false;
    private final int FILE_ROLLOVER_TASK_INTERVAL = Integer.parseInt
        (System.getProperty("gemfire.HDFSRegionDirector.FILE_ROLLOVER_TASK_INTERVAL_SECONDS", "60"));
    
    private SystemTimer hoplogCloseTimer = null;
    
    // instance of hdfs statistics object for this hdfs based region. This
    // object will collect usage and performance related statistics.
    private final SortedOplogStatistics hdfsStats;

    /*
     * An instance of organizer is created for each bucket of regionName region residing on this
     * node. This member maps bucket id with its corresponding organizer instance. A lock is used to
     * manage concurrent writes to the map.
     */
    private ConcurrentMap<Integer, HoplogOrganizer> bucketOrganizerMap;
    
    private HDFSRegionDirector hdfsRegionDirector;

    /**
     * @param listener
     *          listener of change events like file creation and deletion
     * @param hdfsRegionDirector 
     */
    HdfsRegionManager(LocalRegion region, HDFSStoreImpl store,
        HoplogListener listener, StatisticsFactory statsFactory, HDFSRegionDirector hdfsRegionDirector) {
      bucketOrganizerMap = new ConcurrentHashMap<Integer, HoplogOrganizer>();
      this.region = region;
      this.listener = listener;
      this.store = store;
      this.hdfsStats = new SortedOplogStatistics(statsFactory, "HDFSRegionStatistics", region.getFullPath());
      this.hdfsRegionDirector = hdfsRegionDirector;
    }

    public void closeWriters(int minSizeForFileRollover) throws IOException {
      final long startTime = System.currentTimeMillis();
      long elapsedTime = 0;
        
      Collection<HoplogOrganizer> organizers = bucketOrganizerMap.values();
      
      for (HoplogOrganizer organizer : organizers) {
      
        try {
          this.getRegion().checkReadiness();
        } catch (Exception e) {
          break;
        }
        
        ((HDFSUnsortedHoplogOrganizer)organizer).synchronizedCloseWriter(true, 0, 
            minSizeForFileRollover);
      }
      
    }

    public synchronized <T extends PersistedEventImpl> HoplogOrganizer<T> create(int bucketId) throws IOException {
      assert !bucketOrganizerMap.containsKey(bucketId);

      HoplogOrganizer<?> organizer = region.getHDFSWriteOnly() 
          ? new HDFSUnsortedHoplogOrganizer(this, bucketId) 
          : new HdfsSortedOplogOrganizer(this, bucketId);

      bucketOrganizerMap.put(bucketId, organizer);
      // initialize a timer that periodically closes the hoplog writer if the 
      // time for rollover has passed. It also has the responsibility to fix the files.  
      if (this.region.getHDFSWriteOnly() && 
          hoplogCloseTimer == null) {
        hoplogCloseTimer = new SystemTimer(hdfsRegionDirector.
            getCache().getDistributedSystem(), true);
        
        // schedule the task to fix the files that were not closed properly 
        // last time. 
        hoplogCloseTimer.scheduleAtFixedRate(new CloseTmpHoplogsTimerTask(this), 
            1000, FILE_ROLLOVER_TASK_INTERVAL * 1000);
        
        if (logger.isDebugEnabled()) {
          logger.debug("{}Schedulng hoplog rollover timer with interval "+ FILE_ROLLOVER_TASK_INTERVAL + 
              " for hoplog organizer for " + region.getFullPath()
              + ":" + bucketId + " " + organizer, logPrefix);
        }
      }
      
      if (logger.isDebugEnabled()) {
        logger.debug("{}Constructed hoplog organizer for " + region.getFullPath()
            + ":" + bucketId + " " + organizer, logPrefix);
      }
      return (HoplogOrganizer<T>) organizer;
    }
    
    public synchronized <T extends PersistedEventImpl> void addOrganizer(
        int bucketId, HoplogOrganizer<T> organizer) {
      if (bucketOrganizerMap.containsKey(bucketId)) {
        throw new IllegalArgumentException();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}added pre constructed organizer " + region.getFullPath()
            + ":" + bucketId + " " + organizer, logPrefix);
      }
      bucketOrganizerMap.put(bucketId, organizer);
    }

    public void close() {
      closed = true;
      
      if (this.region.getHDFSWriteOnly() && 
          hoplogCloseTimer != null) {
        hoplogCloseTimer.cancel();
        hoplogCloseTimer = null;
      }
      for (int bucket : bucketOrganizerMap.keySet()) {
        close(bucket);
      }
    }
    
    public boolean isClosed() {
      return closed;
    }

    public synchronized void close(int bucketId) {
      try {
        HoplogOrganizer organizer = bucketOrganizerMap.remove(bucketId);
        if (organizer != null) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}Closing hoplog organizer for " + region.getFullPath() + ":" + 
                bucketId + " " + organizer, logPrefix);
          }
          organizer.close();
        }
      } catch (IOException e) {
        if (logger.isDebugEnabled()) {
          logger.debug(logPrefix + "Error closing hoplog organizer for " + region.getFullPath() + ":" + bucketId, e);
        }
      }
      //TODO abort compaction and flush requests for this region
    }
    
    public static String getRegionFolder(String regionPath) {
      String folder = regionPath;
      //Change any underscore into a double underscore
      folder = folder.replace("_", "__");
      //get rid of the leading slash
      folder = folder.replaceFirst("^/", "");
      //replace slashes with underscores
      folder = folder.replace('/', '_');
      return folder;
    }

    public String getRegionFolder() {
      return getRegionFolder(region.getFullPath());
    }

    public HoplogListener getListener() {
      return listener;
    }

    public HDFSStoreImpl getStore() {
      return store;
    }

    public LocalRegion getRegion() {
      return region;
    }
    
    public SortedOplogStatistics getHdfsStats() {
      return hdfsStats;
    }
    
    public Collection<HoplogOrganizer> getBucketOrganizers(){
      return this.bucketOrganizerMap.values();
    }

    /**
     * get the HoplogOrganizers only for the given set of buckets
     */
    public Collection<HoplogOrganizer> getBucketOrganizers(Set<Integer> buckets){
      Set<HoplogOrganizer> result = new HashSet<HoplogOrganizer>();
      for (Integer bucketId : buckets) {
        result.add(this.bucketOrganizerMap.get(bucketId));
      }
      return result;
    }

    /**
     * Delete all files from HDFS for this region. This method
     * should be called after all members have destroyed their
     * region in gemfire, so there should be no threads accessing
     * these files.
     * @throws IOException 
     */
    public void destroyData() throws IOException {
      //Make sure everything is shut down and closed.
      close();
      if (store == null) {
        return;
      }
      Path regionPath = new Path(store.getHomeDir(), getRegionFolder());
      
      //Delete all files in HDFS.
      FileSystem fs = getStore().getFileSystem();
      if(!fs.delete(regionPath, true)) {
        if(fs.exists(regionPath)) {
          throw new IOException("Unable to delete " + regionPath);
        }
      }
    }

    public void performMaintenance() throws IOException {
      Collection<HoplogOrganizer> buckets = getBucketOrganizers();
      for (HoplogOrganizer bucket : buckets) {
        bucket.performMaintenance();
      }
    }
  }
  
  private class JanitorTask implements Runnable {
    boolean terminated = false;
    @Override
    public void run() {
      if (terminated) {
        return;
      }
      fineLog("Executing HDFS Region janitor task", null);
      
      Collection<HdfsRegionManager> regions = regionManagerMap.values();
      for (HdfsRegionManager region : regions) {
        fineLog("Maintaining region:" + region.getRegionFolder(), null);
        try {
          region.performMaintenance();
        } catch (Throwable e) {
          logger.info(LocalizedMessage.create(LocalizedStrings.HOPLOG_IO_ERROR , region.getRegionFolder()));
          logger.info(LocalizedMessage.create(LocalizedStrings.ONE_ARG, e.getMessage()));
          fineLog(null, e);
        }
      }
    }

    public void terminate() {
      terminated = true;
    }
  }
  
  protected static void fineLog(String message, Throwable e) {
    if(logger.isDebugEnabled()) {
      logger.debug(message, e);
    }
  }
}
