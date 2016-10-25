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
package org.apache.geode.cache.util;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.annotations.Experimental;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.support.CronSequenceGenerator;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.internal.logging.LogService;

/**
 * Re-balancing operation relocates data from heavily loaded members to lightly
 * loaded members. In most cases, the decision to re-balance is based on the
 * size of the member and a few other statistics. {@link AutoBalancer} monitors
 * these statistics and if necessary, triggers a re-balancing request.
 * Auto-Balancing is expected to prevent failures and data loss.
 * 
 * <P>
 * This implementation is based on {@code Initializer} implementation. By
 * default auto-balancing is disabled. A user needs to configure
 * {@link AutoBalancer} during cache initialization
 * {@link GemFireCache#getInitializer()}
 * 
 * <P>
 * In a cluster only one member owns auto-balancing responsibility. This is
 * achieved by grabbing a distributed lock. In case of a failure a new member
 * will grab the lock and manage auto balancing.
 * 
 * <P>
 * {@link AutoBalancer} can be controlled using the following configurations
 * <OL>
 * <LI>{@link AutoBalancer#SCHEDULE}
 * <LI>TBD THRESHOLDS
 * 
 */
@Experimental("The autobalancer may be removed or the API may change in future releases")
public class AutoBalancer implements Declarable {
  /**
   * Use this configuration to manage out-of-balance audit frequency. If the
   * auditor finds the system to be out-of-balance, it will trigger
   * re-balancing. Any valid cron string is accepted. The sub-expressions
   * represent the following:
   * <OL>
   * <LI>Seconds
   * <LI>Minutes
   * <LI>Hours
   * <LI>Day-of-Month
   * <LI>Month
   * <LI>Day-of-Week
   * <LI>Year (optional field)
   * 
   * <P>
   * For. e.g. {@code 0 0 * * * ?} for auditing the system every hour
   */
  public static final String SCHEDULE = "schedule";

  /**
   * Use this configuration to manage re-balance invocation. Rebalance operation
   * will be triggered if the total number of bytes rebalance operation may move
   * is more than this threshold, in percentage of the total data size.
   * <P>
   * Default value {@link #DEFAULT_SIZE_THRESHOLD_PERCENT}
   */
  public static final String SIZE_THRESHOLD_PERCENT = "size-threshold-percent";

  /**
   * Default value of {@link AutoBalancer#SIZE_THRESHOLD_PERCENT}. If 10% of
   * data is misplaced, its a good time to redistribute buckets
   */
  public static final int DEFAULT_SIZE_THRESHOLD_PERCENT = 10;

  /**
   * In the initial data load phases,
   * {@link AutoBalancer#SIZE_THRESHOLD_PERCENT} based rebalance invocation may
   * be unnecessary. Rebalance should not be triggered if the total data size
   * managed by cluster is too small. Rebalance operation will be triggered if
   * the total number of bytes rebalance operation may move is more than this
   * number of bytes.
   * <P>
   * Default value {@link #DEFAULT_MINIMUM_SIZE}
   */
  public static final String MINIMUM_SIZE = "minimum-size";

  /**
   * Default value of {@link AutoBalancer#MINIMUM_SIZE}. In the initial data
   * load phases, {@link AutoBalancer#SIZE_THRESHOLD_PERCENT} based rebalance
   * invocation may be unnecessary. Do not rebalance if the data to be moved is
   * less than 100MB
   */
  public static final int DEFAULT_MINIMUM_SIZE = 100 * 1024 * 1024;

  /**
   * Name of the DistributedLockService that {@link AutoBalancer} will use to
   * guard against concurrent maintenance activity
   */
  public static final String AUTO_BALANCER_LOCK_SERVICE_NAME = "__AUTO_B";

  public static final Object AUTO_BALANCER_LOCK = "__AUTO_B_LOCK";

  private final AuditScheduler scheduler;
  private final OOBAuditor auditor;
  private final TimeProvider clock;
  private final CacheOperationFacade cacheFacade;

  private static final Logger logger = LogService.getLogger();

  public AutoBalancer() {
    this(null, null, null, null);
  }

  public AutoBalancer(AuditScheduler scheduler, OOBAuditor auditor, TimeProvider clock,
      CacheOperationFacade cacheFacade) {
    this.cacheFacade = cacheFacade == null ? new GeodeCacheFacade() : cacheFacade;
    this.scheduler = scheduler == null ? new CronScheduler() : scheduler;
    this.auditor = auditor == null ? new SizeBasedOOBAuditor(this.cacheFacade) : auditor;
    this.clock = clock == null ? new SystemClockTimeProvider() : clock;
  }

  @Override
  public void init(Properties props) {
    if (logger.isDebugEnabled()) {
      logger.debug("Initializing " + this.getClass().getSimpleName() + " with " + props);
    }

    auditor.init(props);

    String schedule = null;
    if (props != null) {
      schedule = props.getProperty(SCHEDULE);
    }
    scheduler.init(schedule);
  }

  /**
   * Invokes audit triggers based on a cron schedule.
   * <OL>
   * <LI>computes delay = next slot - current time
   * <LI>schedules a out-of-balance audit task to be started after delay
   * computed earlier
   * <LI>once the audit task completes, it repeats delay computation and task
   * submission
   */
  private class CronScheduler implements AuditScheduler {
    final ScheduledExecutorService trigger;
    CronSequenceGenerator generator;

    CronScheduler() {
      trigger = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r, "AutoBalancer");
          thread.setDaemon(true);
          return thread;
        }
      });
    }

    @Override
    public void init(String schedule) {
      if (logger.isDebugEnabled()) {
        logger.debug("Initializing " + this.getClass().getSimpleName() + " with " + schedule);
      }

      if (schedule == null || schedule.isEmpty()) {
        throw new GemFireConfigException("Missing configuration: " + SCHEDULE);
      }

      try {
        generator = new CronSequenceGenerator(schedule);
      } catch(Exception e) {
        throw new GemFireConfigException("Cron expression could not be parsed: " + schedule, e);
      }

      submitNext();
    }

    private void submitNext() {
      long currentTime = clock.currentTimeMillis();
      Date nextSchedule = generator.next(new Date(currentTime));
      long delay = nextSchedule.getTime() - currentTime;

      if (logger.isDebugEnabled()) {
        logger.debug("Now={}, next audit time={}, delay={} ms", new Date(currentTime), nextSchedule, delay);
      }

      trigger.schedule(new Runnable() {
        @Override
        public void run() {
          try {
            auditor.execute();
          } catch (CacheClosedException e) {
            logger.warn("Cache closed while attempting to rebalance the cluster. Abort future jobs", e);
            return;
          } catch (Exception e) {
            logger.warn("Error while executing out-of-balance audit.", e);
          }
          submitNext();
        }
      }, delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() {
      trigger.shutdownNow();
    }
  }

  /**
   * Queries member statistics and health to determine if a re-balance operation
   * is needed
   * <OL>
   * <LI>acquires distributed lock
   * <LI>queries member health
   * <LI>updates auto-balance stat
   * <LI>release lock
   */
  static class SizeBasedOOBAuditor implements OOBAuditor {
    private int sizeThreshold = DEFAULT_SIZE_THRESHOLD_PERCENT;
    private int sizeMinimum = DEFAULT_MINIMUM_SIZE;

    final CacheOperationFacade cache;

    public SizeBasedOOBAuditor(CacheOperationFacade cache) {
      this.cache = cache;
    }

    @Override
    public void init(Properties props) {
      if (logger.isDebugEnabled()) {
        logger.debug("Initializing " + this.getClass().getSimpleName());
      }

      if (props != null) {
        if (props.getProperty(SIZE_THRESHOLD_PERCENT) != null) {
          sizeThreshold = Integer.valueOf(props.getProperty(SIZE_THRESHOLD_PERCENT));
          if (sizeThreshold <= 0 || sizeThreshold >= 100) {
            throw new GemFireConfigException(SIZE_THRESHOLD_PERCENT + " should be integer, 1 to 99");
          }
        }
        if (props.getProperty(MINIMUM_SIZE) != null) {
          sizeMinimum = Integer.valueOf(props.getProperty(MINIMUM_SIZE));
          if (sizeMinimum <= 0) {
            throw new GemFireConfigException(MINIMUM_SIZE + " should be greater than 0");
          }
        }
      }
    }

    @Override
    public void execute() {
      boolean result = cache.acquireAutoBalanceLock();
      if (!result) {
        if (logger.isDebugEnabled()) {
          logger.debug("Another member owns auto-balance lock. Skip this attempt to rebalance the cluster");
        }
        return;
      }

      cache.incrementAttemptCounter();
      result = needsRebalancing();
      if (!result) {
        if (logger.isDebugEnabled()) {
          logger.debug("Rebalancing is not needed");
        }
        return;
      }

      cache.rebalance();
    }

    /**
     * By default auto-balancer will avoid rebalancing, because a user can
     * always trigger rebalance manually. So in case of error or inconsistent
     * data, return false. Return true if
     * <OL>
     * <LI>total transfer size is above threshold percent of total data size at
     * cluster level
     * <LI>If some smaller capacity nodes are heavily loaded while bigger
     * capacity nodes are balanced. In such a scenario transfer size based
     * trigger may not cause rebalance.
     */
    boolean needsRebalancing() {
      // test cluster level status
      long transferSize = cache.getTotalTransferSize();
      if (transferSize <= sizeMinimum) {
        return false;
      }

      Map<PartitionedRegion, InternalPRInfo> details = cache.getRegionMemberDetails();
      long totalSize = cache.getTotalDataSize(details);

      if (totalSize > 0) {
        int transferPercent = (int) ((100.0 * transferSize) / totalSize);
        if (transferPercent >= sizeThreshold) {
          return true;
        }
      }

      // TODO test member level skew

      return false;
    }

    int getSizeThreshold() {
      return sizeThreshold;
    }

    public long getSizeMinimum() {
      return sizeMinimum;
    }
  }

  /**
   * Hides cache level details and exposes simple methods relevant for
   * auto-balancing
   */
  static class GeodeCacheFacade implements CacheOperationFacade {
    private final AtomicBoolean isLockAcquired = new AtomicBoolean(false);

    private GemFireCacheImpl cache;

    public GeodeCacheFacade() {
      this(null);
    }

    public GeodeCacheFacade(GemFireCacheImpl cache) {
      this.cache = cache;
    }

    @Override
    public Map<PartitionedRegion, InternalPRInfo> getRegionMemberDetails() {
      GemFireCacheImpl cache = getCache();
      Map<PartitionedRegion, InternalPRInfo> detailsMap = new HashMap<>();
      for (PartitionedRegion region : cache.getPartitionedRegions()) {
        LoadProbe probe = cache.getResourceManager().getLoadProbe();
        InternalPRInfo info = region.getRedundancyProvider().buildPartitionedRegionInfo(true, probe);
        detailsMap.put(region, info);
      }
      return detailsMap;
    }

    @Override
    public long getTotalDataSize(Map<PartitionedRegion, InternalPRInfo> details) {
      long totalSize = 0;
      if (details != null) {
        for (PartitionedRegion region : details.keySet()) {
          InternalPRInfo info = details.get(region);
          Set<PartitionMemberInfo> membersInfo = info.getPartitionMemberInfo();
          for (PartitionMemberInfo member : membersInfo) {
            if (logger.isDebugEnabled()) {
              logger.debug("Region:{}, Member: {}, Size: {}", region.getFullPath(), member, member.getSize());
            }
            totalSize += member.getSize();
          }
        }
      }
      return totalSize;
    }

    @Override
    public long getTotalTransferSize() {
      try {
        RebalanceOperation operation = getCache().getResourceManager().createRebalanceFactory().simulate();
        RebalanceResults result = operation.getResults();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Rebalance estimate: RebalanceResultsImpl [TotalBucketCreateBytes=" + result.getTotalBucketCreateBytes()
                  + ", TotalBucketCreatesCompleted=" + result.getTotalBucketCreatesCompleted()
                  + ", TotalBucketTransferBytes=" + result.getTotalBucketTransferBytes()
                  + ", TotalBucketTransfersCompleted=" + result.getTotalBucketTransfersCompleted()
                  + ", TotalPrimaryTransfersCompleted=" + result.getTotalPrimaryTransfersCompleted() + "]");
        }
        return result.getTotalBucketTransferBytes();
      } catch (CancellationException e) {
        logger.info("Error while trying to estimate rebalance cost ", e);
      } catch (InterruptedException e) {
        logger.info("Error while trying to estimate rebalance cost ", e);
      }
      return 0;
    }

    @Override
    public void incrementAttemptCounter() {
      GemFireCacheImpl cache = getCache();
      try {
        cache.getResourceManager().getStats().incAutoRebalanceAttempts();
      } catch (Exception e) {
        logger.warn("Failed to increment AutoBalanceAttempts counter");
      }
    }

    @Override
    public void rebalance() {
      try {
        RebalanceOperation operation = getCache().getResourceManager().createRebalanceFactory().start();
        RebalanceResults result = operation.getResults();
        logger.info("Rebalance result: [TotalBucketCreateBytes=" + result.getTotalBucketCreateBytes()
            + ", TotalBucketCreateTime=" + result.getTotalBucketCreateTime() + ", TotalBucketCreatesCompleted="
            + result.getTotalBucketCreatesCompleted() + ", TotalBucketTransferBytes="
            + result.getTotalBucketTransferBytes() + ", TotalBucketTransferTime=" + result.getTotalBucketTransferTime()
            + ", TotalBucketTransfersCompleted=" + +result.getTotalBucketTransfersCompleted()
            + ", TotalPrimaryTransferTime=" + result.getTotalPrimaryTransferTime() + ", TotalPrimaryTransfersCompleted="
            + result.getTotalPrimaryTransfersCompleted() + ", TotalTime=" + result.getTotalTime() + "]");
      } catch (CancellationException e) {
        logger.info("Error rebalancing the cluster", e);
      } catch (InterruptedException e) {
        logger.info("Error rebalancing the cluster", e);
      }
    }

    GemFireCacheImpl getCache() {
      if (cache == null) {
        synchronized (this) {
          if (cache == null) {
            cache = GemFireCacheImpl.getInstance();
            if (cache == null) {
              throw new IllegalStateException("Missing cache instance.");
            }
          }
        }
      }
      if (cache.isClosed()) {
        throw new CacheClosedException();
      }
      return cache;
    }

    @Override
    public boolean acquireAutoBalanceLock() {
      if (!isLockAcquired.get()) {
        synchronized (isLockAcquired) {
          if (!isLockAcquired.get()) {
            DistributedLockService dls = getDLS();

            boolean result = dls.lock(AUTO_BALANCER_LOCK, 0, -1);
            if (result) {
              isLockAcquired.set(true);
              if (logger.isDebugEnabled()) {
                logger.debug("Grabbed AutoBalancer lock");
              }
            } else {
              if (logger.isDebugEnabled()) {
                logger.debug("Another member owns auto-balance lock. Skip this attempt to rebalance the cluster");
              }
            }
          }
        }
      }
      return isLockAcquired.get();
    }

    @Override
    public DistributedLockService getDLS() {
      GemFireCacheImpl cache = getCache();
      DistributedLockService dls = DistributedLockService.getServiceNamed(AUTO_BALANCER_LOCK_SERVICE_NAME);
      if (dls == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Creating DistributeLockService");
        }
        dls = DLockService.create(AUTO_BALANCER_LOCK_SERVICE_NAME, cache.getDistributedSystem(), true, true, true);
      }

      return dls;
    }
  }

  private class SystemClockTimeProvider implements TimeProvider {
    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  }

  interface AuditScheduler {
    void init(String schedule);

    void destroy();
  }

  interface OOBAuditor {
    void init(Properties props);

    void execute();
  }

  interface TimeProvider {
    long currentTimeMillis();
  }

  interface CacheOperationFacade {
    boolean acquireAutoBalanceLock();

    DistributedLockService getDLS();

    void rebalance();

    void incrementAttemptCounter();

    Map<PartitionedRegion, InternalPRInfo> getRegionMemberDetails();

    long getTotalDataSize(Map<PartitionedRegion, InternalPRInfo> details);

    long getTotalTransferSize();
  }

  OOBAuditor getOOBAuditor() {
    return auditor;
  }

  public CacheOperationFacade getCacheOperationFacade() {
    return this.cacheFacade;
  }

  public void destroy() {
    scheduler.destroy();
  }
}
