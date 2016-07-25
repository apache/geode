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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

public class DiskStoreMonitor {
  private static final Logger logger = LogService.getLogger();

  private static final boolean DISABLE_MONITOR = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DISK_USAGE_DISABLE_MONITORING");
//  private static final boolean AUTO_RECONNECT = Boolean.getBoolean("gemfire.DISK_USAGE_ENABLE_AUTO_RECONNECT");

  private static final int USAGE_CHECK_INTERVAL = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "DISK_USAGE_POLLING_INTERVAL_MILLIS", 10000);
  private static final float LOG_WARNING_THRESHOLD_PCT = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "DISK_USAGE_LOG_WARNING_PERCENT", 99);

  enum DiskState {
    NORMAL, 
    WARN,
    CRITICAL;
    
    public static DiskState select(double actual, double warn, double critical, boolean belowMinimum) {
      if (critical > 0 && (actual > critical || belowMinimum)) {
        return CRITICAL;
      } else if (warn > 0 && actual > warn) {
        return WARN;
      }
      return NORMAL;
    }
  }

  /**
   * Validates the warning percent.
   * @param val the value to check
   */
  public static void checkWarning(float val) {
    if (val < 0 || val > 100) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_DISK_USAGE_WARNING_INVALID_0.toLocalizedString(Float.valueOf(val)));
    }
  }

  /**
   * Validates the critical percent.
   * @param val the value to check
   */
  public static void checkCritical(float val) {
    if (val < 0 || val > 100) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_DISK_USAGE_CRITICAL_INVALID_0.toLocalizedString(Float.valueOf(val)));
    }
  }
    
  private final ScheduledExecutorService exec;

  private final Map<DiskStoreImpl, Set<DirectoryHolderUsage>> disks;
  private final LogUsage logDisk;
  
//  // this is set when we go into auto_reconnect mode
//  private volatile DirectoryHolderUsage criticalDisk;
  
  volatile DiskStateAction _testAction;
  interface DiskStateAction {
    void handleDiskStateChange(DiskState state);
  }

  public DiskStoreMonitor() {
    disks = new ConcurrentHashMap<DiskStoreImpl, Set<DirectoryHolderUsage>>();
    logDisk = new LogUsage(getLogDir());
    
    if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR)) {
      logger.trace(LogMarker.DISK_STORE_MONITOR, "Disk monitoring is {}", (DISABLE_MONITOR ? "disabled" : "enabled")); 
      logger.trace(LogMarker.DISK_STORE_MONITOR, "Log directory usage warning is set to {}%", LOG_WARNING_THRESHOLD_PCT); 
      logger.trace(LogMarker.DISK_STORE_MONITOR, "Scheduling disk usage checks every {} ms", USAGE_CHECK_INTERVAL); 
    }

    if (DISABLE_MONITOR) {
      exec = null;
    } else {
      final ThreadGroup tg = LoggingThreadGroup .createThreadGroup(
          LocalizedStrings.DiskStoreMonitor_ThreadGroup.toLocalizedString(), logger);
      exec = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(tg, r, "DiskStoreMonitor");
          t.setDaemon(true);
          return t;
        }
      });
      
      // always monitor the log dir, even if there are no disk stores
      exec.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            checkUsage();
          } catch (Exception e) {
            logger.error(LocalizedMessage.create(LocalizedStrings.DiskStoreMonitor_ERR), e);
          }
        }
      }, 0, USAGE_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }
  }
  
  public void addDiskStore(DiskStoreImpl ds) {
    if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR)) {
      logger.trace(LogMarker.DISK_STORE_MONITOR, "Now monitoring disk store {}", ds.getName()); 
    }

    Set<DirectoryHolderUsage> du = new HashSet<DirectoryHolderUsage>();
    for (DirectoryHolder dir : ds.getDirectoryHolders()) {
      du.add(new DirectoryHolderUsage(ds, dir));
    }
    disks.put(ds, du);
  }
  
  public void removeDiskStore(DiskStoreImpl ds) {
    if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR)) {
      logger.trace(LogMarker.DISK_STORE_MONITOR, "No longer monitoring disk store {}", ds.getName()); 
    }

    disks.remove(ds);
  }

  public boolean isNormal(DiskStoreImpl ds, DirectoryHolder dir) {
    Set<DirectoryHolderUsage> dirs = disks.get(ds);
    if (dirs != null) {
      for (DirectoryHolderUsage du : dirs) {
        if (du.dir == dir) {
          return du.getState() == DiskState.NORMAL;
        }
      }
    }
    
    // only a postive negatory :-)
    return true;
  }
  
  public void close() {
    // only shutdown if we're not waiting for the critical disk to return to normal
    if (exec != null /* && criticalDisk == null */) {
      exec.shutdownNow();
    }
    disks.clear();
  }
  
  private void checkUsage() {
//    // 1) Check critical disk if needed
//    if (criticalDisk != null) {
//      criticalDisk.update(
//          criticalDisk.disk.getDiskUsageWarningPercentage(), 
//          criticalDisk.disk.getDiskUsageCriticalPercentage());
//      return;
//    }
      
    // 2) Check disk stores / dirs
    for (Entry<DiskStoreImpl, Set<DirectoryHolderUsage>> entry : disks.entrySet()) {
      DiskStoreImpl ds = entry.getKey();
      for (DiskUsage du : entry.getValue()) {
        DiskState update = du.update(ds.getDiskUsageWarningPercentage(), ds.getDiskUsageCriticalPercentage());
        if (update == DiskState.CRITICAL) {
          break;
        }
      }
    }
    
    // 3) Check log dir
    logDisk.update(LOG_WARNING_THRESHOLD_PCT, 100);
  }      

  private File getLogDir() {
    File log = null;
    GemFireCacheImpl gci = GemFireCacheImpl.getInstance();
    if (gci != null) {
      InternalDistributedSystem ds = gci.getDistributedSystem();
      if (ds != null) {
        DistributionConfig conf = ds.getConfig();
        if (conf != null) {
          log = conf.getLogFile();
          if (log != null) {
            log = log.getParentFile();
          }
        }
      }
    }
    
    if (log == null) {
      // assume current directory
      log = new File(".");
    }
    return log;
  }

  abstract class DiskUsage {
    private DiskState state;
    
    DiskUsage() {
      state = DiskState.NORMAL;
    }
    
    public synchronized DiskState getState() {
      return state;
    }

    public DiskState update(float warning, float critical) {
      DiskState current;
      synchronized (this) {
        current = state;
      }
      
      // don't bother checking if the the limits are disabled
      if (!(warning > 0 || critical > 0)) {
        return current;
      }
      
      if (!dir().exists()) {
        if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR)) {
          logger.trace(LogMarker.DISK_STORE_MONITOR, "Skipping check of non-existent directory {}", dir().getAbsolutePath()); 
        }
        return current;
      }
      
      long min = getMinimumSpace();
      if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR)) {
        logger.trace(LogMarker.DISK_STORE_MONITOR, "Checking usage for directory {}, minimum free space is {} MB", dir().getAbsolutePath(), min); 
      }
      
      long start = System.nanoTime();
      long remaining = dir().getUsableSpace();
      long total = dir().getTotalSpace();
      long elapsed = System.nanoTime() - start;

      double use = 100.0 * (total - remaining) / total;
      recordStats(total, remaining, elapsed);
      
      String pct = Math.round(use) + "%";
      if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR)) {
        logger.trace(LogMarker.DISK_STORE_MONITOR, "Directory {} has {} bytes free out of {} ({} usage)", dir().getAbsolutePath(), remaining, total, pct);
      }

      boolean belowMin = remaining < 1024 * 1024 * min;
      DiskState next = DiskState.select(use, warning, critical, belowMin);
      if (next == current) {
        return next;
      }
      
      synchronized (this) {
        state = next;
      }
      
      handleStateChange(next, pct);
      return next;
    }
    
    protected abstract File dir();
    protected abstract long getMinimumSpace();
    protected abstract void recordStats(long total, long free, long elapsed);
    protected abstract void handleStateChange(DiskState next, String pct);
  }
  
  class LogUsage extends DiskUsage {
    private final File dir;

    public LogUsage(File dir) {
      this.dir = dir;
    }
    
    protected void handleStateChange(DiskState next, String pct) {
      Object[] args = new Object[] { dir.getAbsolutePath(), pct };
      switch (next) {
      case NORMAL:
        logger.info(LogMarker.DISK_STORE_MONITOR, LocalizedMessage.create(LocalizedStrings.DiskStoreMonitor_LOG_DISK_NORMAL, args));
        break;
      
      case WARN:
      case CRITICAL:
        logger.warn(LogMarker.DISK_STORE_MONITOR, LocalizedMessage.create(LocalizedStrings.DiskStoreMonitor_LOG_DISK_WARNING, args));
        break;
      }
    }

    @Override
    protected long getMinimumSpace() {
      return DiskStoreImpl.MIN_DISK_SPACE_FOR_LOGS;
    }

    @Override
    protected File dir() {
      return dir;
    }

    @Override
    protected void recordStats(long total, long free, long elapsed) {
    }
  }
  
  class DirectoryHolderUsage extends DiskUsage {
    private final DiskStoreImpl disk;
    private final DirectoryHolder dir;

    public DirectoryHolderUsage(DiskStoreImpl disk, DirectoryHolder dir) {
      this.disk = disk;
      this.dir = dir;
    }

    protected void handleStateChange(DiskState next, String pct) {
      if (_testAction != null) {
        logger.info(LogMarker.DISK_STORE_MONITOR, "Invoking test handler for state change to {}", next);
        _testAction.handleDiskStateChange(next);
      }

      Object[] args = new Object[] { dir.getDir(), disk.getName(), pct };
      String msg = "Critical disk usage threshold exceeded for volume "
          + dir.getDir().getAbsolutePath() + ": " + pct + " full";
      
      switch (next) {
      case NORMAL:
        logger.warn(LogMarker.DISK_STORE_MONITOR, LocalizedMessage.create(LocalizedStrings.DiskStoreMonitor_DISK_NORMAL, args));
        
//        // try to restart cache after we return to normal operations
//        if (AUTO_RECONNECT && this == criticalDisk) {
//          performReconnect(msg);
//        }
        break;
      
      case WARN:
        logger.warn(LogMarker.DISK_STORE_MONITOR, LocalizedMessage.create(LocalizedStrings.DiskStoreMonitor_DISK_WARNING, args));
        break;
  
      case CRITICAL:
        logger.error(LogMarker.DISK_STORE_MONITOR, LocalizedMessage.create(LocalizedStrings.DiskStoreMonitor_DISK_CRITICAL, args));
        
        try {
//          // prepare for restart
//          if (AUTO_RECONNECT) {
//            disk.getCache().saveCacheXmlForReconnect();
//            criticalDisk = this;
//          }
        } finally {
          // pull the plug
          disk.handleDiskAccessException(new DiskAccessException(msg, disk));
        }
        break;
      }
    }

//    private void performReconnect(String msg) {
//      try {
//        // don't try to reconnect before the cache is closed
//        disk._testHandleDiskAccessException.await();
//        
//        // now reconnect, clear out the var first so a close can interrupt the
//        // reconnect
//        criticalDisk = null;
//        boolean restart = disk.getCache().getDistributedSystem().tryReconnect(true, msg, disk.getCache());
//        if (LogMarker.DISK_STORE_MONITOR || logger.isDebugEnabled()) {
//          String pre = restart ? "Successfully" : "Unsuccessfully";
//          logger.info(LocalizedStrings.DEBUG, pre + " attempted to restart cache");
//        }
//      } catch (InterruptedException e) {
//        Thread.currentThread().interrupt();
//      } finally {
//        close();
//      }
//    }

    @Override
    protected File dir() {
      return dir.getDir();
    }

    @Override
    protected long getMinimumSpace() {
      return DiskStoreImpl.MIN_DISK_SPACE_FOR_LOGS + disk.getMaxOplogSize();
    }

    @Override
    protected void recordStats(long total, long free, long elapsed) {
      dir.getDiskDirectoryStats().addVolumeCheck(total, free, elapsed);
    }
  }
}
