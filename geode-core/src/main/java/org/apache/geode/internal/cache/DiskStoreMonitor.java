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
package org.apache.geode.internal.cache;

import java.io.File;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class DiskStoreMonitor {
  private static final Logger logger = LogService.getLogger();

  private static final int USAGE_CHECK_INTERVAL = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "DISK_USAGE_POLLING_INTERVAL_MILLIS", 10000);

  private static final float LOG_WARNING_THRESHOLD_PCT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "DISK_USAGE_LOG_WARNING_PERCENT", 99);

  enum DiskState {
    NORMAL, WARN, CRITICAL;

    public static DiskState select(double actual, double warn, double critical,
        boolean belowMinimum) {
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
   *
   * @param val the value to check
   */
  public static void checkWarning(float val) {
    if (val < 0 || val > 100) {
      throw new IllegalArgumentException(
          String.format(
              "Disk usage warning percentage must be set to a value between 0-100.  The value %s is invalid.",
              val));
    }
  }

  /**
   * Validates the critical percent.
   *
   * @param val the value to check
   */
  public static void checkCritical(float val) {
    if (val < 0 || val > 100) {
      throw new IllegalArgumentException(
          String.format(
              "Disk usage critical percentage must be set to a value between 0-100.  The value %s is invalid.",
              val));
    }
  }

  static final String DISK_USAGE_DISABLE_MONITORING =
      DistributionConfig.GEMFIRE_PREFIX + "DISK_USAGE_DISABLE_MONITORING";

  private final boolean disableMonitor = Boolean.getBoolean(DISK_USAGE_DISABLE_MONITORING);

  private final ScheduledExecutorService exec;

  private final Map<DiskStoreImpl, Set<DirectoryHolderUsage>> disks;

  private final LogUsage logDisk;

  volatile DiskStateAction _testAction;

  interface DiskStateAction {
    void handleDiskStateChange(DiskState state);
  }

  public DiskStoreMonitor(File logFile) {
    disks = new ConcurrentHashMap<DiskStoreImpl, Set<DirectoryHolderUsage>>();
    logDisk = new LogUsage(getLogDir(logFile));

    if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR_VERBOSE)) {
      logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE, "Disk monitoring is {}",
          (disableMonitor ? "disabled" : "enabled"));
      logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE,
          "Log directory usage warning is set to {}%", LOG_WARNING_THRESHOLD_PCT);
      logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE, "Scheduling disk usage checks every {} ms",
          USAGE_CHECK_INTERVAL);
    }

    if (disableMonitor) {
      exec = null;
    } else {
      exec = LoggingExecutors.newScheduledThreadPool("DiskStoreMonitor", 1);
      // always monitor the log dir, even if there are no disk stores
      exec.scheduleWithFixedDelay(() -> {
        try {
          checkUsage();
        } catch (Exception e) {
          logger.error("The DiskStore Monitor has encountered an error", e);
        }
      }, 0, USAGE_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    }
  }

  LogUsage getLogDisk() {
    return logDisk;
  }

  public void addDiskStore(DiskStoreImpl ds) {
    if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR_VERBOSE)) {
      logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE, "Now monitoring disk store {}",
          ds.getName());
    }

    Set<DirectoryHolderUsage> du = new HashSet<DirectoryHolderUsage>();
    for (DirectoryHolder dir : ds.getDirectoryHolders()) {
      du.add(new DirectoryHolderUsage(ds, dir));
    }
    disks.put(ds, du);
  }

  public void removeDiskStore(DiskStoreImpl ds) {
    if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR_VERBOSE)) {
      logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE, "No longer monitoring disk store {}",
          ds.getName());
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
    if (exec != null) {
      exec.shutdownNow();
    }
    disks.clear();
  }

  private void checkUsage() {
    // Check disk stores / dirs
    for (Entry<DiskStoreImpl, Set<DirectoryHolderUsage>> entry : disks.entrySet()) {
      DiskStoreImpl ds = entry.getKey();
      for (DiskUsage du : entry.getValue()) {
        DiskState update =
            du.update(ds.getDiskUsageWarningPercentage(), ds.getDiskUsageCriticalPercentage());
        if (update == DiskState.CRITICAL) {
          break;
        }
      }
    }

    // Check log dir
    logDisk.update(LOG_WARNING_THRESHOLD_PCT, 100);
  }

  private static File getLogDir(File logFile) {
    File logDir = null;
    if (logFile != null) {
      logDir = logFile.getParentFile();
    }

    if (logDir == null) {
      // assume current directory
      logDir = new File(".");
    }
    return logDir;
  }

  abstract static class DiskUsage {
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
        if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR_VERBOSE)) {
          logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE,
              "Skipping check of non-existent directory {}", dir().getAbsolutePath());
        }
        return current;
      }

      long minMegabytes = getMinimumSpace();
      final long minBytes = minMegabytes * 1024 * 1024;
      if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR_VERBOSE)) {
        logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE,
            "Checking usage for directory {}, minimum free space is {} MB", dir().getAbsolutePath(),
            minMegabytes);
      }

      long start = System.nanoTime();
      long remaining = dir().getUsableSpace();
      long total = dir().getTotalSpace();
      long elapsed = System.nanoTime() - start;

      double use = 100.0 * (total - remaining) / total;
      recordStats(total, remaining, elapsed);

      final DecimalFormat decimalFormat = new DecimalFormat("#.#");
      String pct = decimalFormat.format(use) + "%";
      if (logger.isTraceEnabled(LogMarker.DISK_STORE_MONITOR_VERBOSE)) {
        logger.trace(LogMarker.DISK_STORE_MONITOR_VERBOSE,
            "Directory {} has {} bytes free out of {} ({} usage)", dir().getAbsolutePath(),
            remaining, total, pct);
      }

      boolean belowMin = remaining < minBytes;
      DiskState next = DiskState.select(use, warning, critical, belowMin);
      if (next == current) {
        return next;
      }

      synchronized (this) {
        state = next;
      }

      String criticalMessage = null;
      if (next == DiskState.CRITICAL) {
        if (belowMin) {
          criticalMessage = "the file system only has " + remaining
              + " bytes free which is below the minimum of " + minBytes + ".";
        } else {
          criticalMessage =
              "the file system is " + pct + " full, which reached the critical threshold of "
                  + decimalFormat.format(critical) + "%.";
        }
      }
      handleStateChange(next, pct, criticalMessage);
      return next;
    }

    protected abstract File dir();

    protected abstract long getMinimumSpace();

    protected abstract void recordStats(long total, long free, long elapsed);

    protected abstract void handleStateChange(DiskState next, String pct, String criticalMessage);
  }

  static class LogUsage extends DiskUsage {
    private final File dir;

    public LogUsage(File dir) {
      this.dir = dir;
    }

    protected void handleStateChange(DiskState next, String pct, String critcalMessage) {
      Object[] args = new Object[] {dir.getAbsolutePath(), pct};
      switch (next) {
        case NORMAL:
          logger.info(LogMarker.DISK_STORE_MONITOR_MARKER,
              "The disk volume {} for log files has returned to normal usage levels and is {} full.",
              args);
          break;

        case WARN:
        case CRITICAL:
          logger.warn(LogMarker.DISK_STORE_MONITOR_MARKER,
              "The disk volume {} for log files has exceeded the warning usage threshold and is {} full.",
              args);
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
    protected void recordStats(long total, long free, long elapsed) {}
  }

  class DirectoryHolderUsage extends DiskUsage {
    private final DiskStoreImpl disk;
    private final DirectoryHolder dir;

    public DirectoryHolderUsage(DiskStoreImpl disk, DirectoryHolder dir) {
      this.disk = disk;
      this.dir = dir;
    }

    protected void handleStateChange(DiskState next, String pct, String criticalMessage) {
      if (_testAction != null) {
        logger.info(LogMarker.DISK_STORE_MONITOR_MARKER,
            "Invoking test handler for state change to {}", next);
        _testAction.handleDiskStateChange(next);
      }

      Object[] args = new Object[] {dir.getDir(), disk.getName(), pct};

      switch (next) {
        case NORMAL:
          logger.warn(LogMarker.DISK_STORE_MONITOR_MARKER,
              "The disk volume {} for disk store {} has returned to normal usage levels and is {} full",
              args);
          break;
        case WARN:
          logger.warn(LogMarker.DISK_STORE_MONITOR_MARKER,
              "The disk volume {} for disk store {} has exceeded the warning usage threshold and is {} full",
              args);
          break;
        case CRITICAL:
          logger.error(LogMarker.DISK_STORE_MONITOR_MARKER,
              "The disk volume {} for disk store {} has exceeded the critical usage threshold and is {} full",
              args);
          String msg = "Critical disk usage threshold exceeded for volume "
              + dir.getDir().getAbsolutePath() + ": " + criticalMessage;
          disk.handleDiskAccessException(new DiskAccessException(msg, disk));
          break;
      }
    }

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
