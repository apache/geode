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
package org.apache.geode.admin.internal;

import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.admin.CacheHealthConfig;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.CacheLifecycleListener;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;

/**
 * Contains the logic for evaluating the health of a GemFire {@code Cache} instance according to the
 * thresholds provided in a {@link CacheHealthConfig}.
 *
 * @since GemFire 3.5
 */
class CacheHealthEvaluator extends AbstractHealthEvaluator implements CacheLifecycleListener {

  private static final Logger logger = LogService.getLogger();

  /** The config from which we get the evaluation criteria */
  private final CacheHealthConfig config;

  /** The description of the cache being evaluated */
  private String description;

  /**
   * Statistics about the {@code Cache} instance. If no cache has been created in this VM, this
   * field will be {@code null}
   */
  private CachePerfStats cacheStats;

  /** The previous value of the netsearchTime stat (in nanoseconds) */
  private long prevNetsearchTime;

  /** The previous value of the netsearchedCompleted stat */
  private long prevNetsearchesCompleted;

  /** The previous value of the loadTime stat (in nanoseconds) */
  private long prevLoadTime;

  /** The previous value of the loadedCompleted stat */
  private long prevLoadsCompleted;

  /** The previous value of the gets stat */
  private long prevGets;

  /**
   * Creates a new {@code CacheHealthEvaluator}
   */
  CacheHealthEvaluator(GemFireHealthConfig config, DistributionManager dm) {
    super(config, dm);

    this.config = config;
    InternalDistributedSystem system = dm.getSystem();
    InternalCache cache;
    try {
      cache = (InternalCache) CacheFactory.getInstance(system);
    } catch (CancelException ignore) {
      // No cache in this VM
      cache = null;
    }

    initialize(cache, dm);
    GemFireCacheImpl.addCacheLifecycleListener(this);
  }

  @Override
  protected String getDescription() {
    return this.description;
  }

  /**
   * Initializes the state of this evaluator based on the given cache instance.
   */
  private void initialize(InternalCache cache, DistributionManager dm) {
    StringBuilder sb = new StringBuilder();
    if (cache != null) {
      this.cacheStats = cache.getCachePerfStats();

      sb.append("Cache \"");
      sb.append(cache.getName());
      sb.append('"');

    } else {
      sb.append("No Cache");
    }

    sb.append(" in member ");
    sb.append(dm.getId());
    int pid = OSProcess.getId();
    if (pid != 0) {
      sb.append(" with pid ");
      sb.append(pid);
    }
    this.description = sb.toString();
  }

  @Override
  public void cacheCreated(InternalCache cache) {
    InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
    DistributionManager dm = system.getDistributionManager();
    initialize(cache, dm);
  }

  /**
   * Checks to make sure that the average {@code netSearch} time during the previous health check
   * interval is less than the {@linkplain CacheHealthConfig#getMaxNetSearchTime threshold}. If not,
   * the status is "okay" health.
   *
   * @see CachePerfStats#getNetsearchTime
   * @see CachePerfStats#getNetsearchesCompleted
   */
  private void checkNetSearchTime(List status) {
    if (this.cacheStats == null || isFirstEvaluation() || this.cacheStats.isClosed()) {
      return;
    }

    long deltaNetsearchTime = this.cacheStats.getNetsearchTime() - this.prevNetsearchTime;
    long deltaNetsearchesCompleted =
        this.cacheStats.getNetsearchesCompleted() - this.prevNetsearchesCompleted;

    if (deltaNetsearchesCompleted != 0) {
      long ratio = deltaNetsearchTime / deltaNetsearchesCompleted;
      ratio /= 1000000;
      long threshold = this.config.getMaxNetSearchTime();

      if (ratio > threshold) {
        String s =
            String.format(
                "The average duration of a Cache netSearch (%s ms) exceeds the threshold (%s ms)",
                ratio, threshold);
        status.add(okayHealth(s));
      }
    }
  }

  /**
   * Checks to make sure that the average {@code load} time during the previous health check
   * interval is less than the {@linkplain CacheHealthConfig#getMaxLoadTime threshold}. If not, the
   * status is "okay" health.
   *
   * @see CachePerfStats#getLoadTime
   * @see CachePerfStats#getLoadsCompleted
   */
  private void checkLoadTime(List<HealthStatus> status) {
    if (this.cacheStats == null || isFirstEvaluation() || this.cacheStats.isClosed()) {
      return;
    }

    if (!isFirstEvaluation()) {
      long deltaLoadTime = this.cacheStats.getLoadTime() - this.prevLoadTime;
      long deltaLoadsCompleted = this.cacheStats.getLoadsCompleted() - this.prevLoadsCompleted;

      if (logger.isDebugEnabled()) {
        logger.debug("Completed {} loads in {} ms", deltaLoadsCompleted, deltaLoadTime / 1000000);
      }

      if (deltaLoadsCompleted != 0) {
        long ratio = deltaLoadTime / deltaLoadsCompleted;
        ratio /= 1000000;
        long threshold = this.config.getMaxLoadTime();

        if (ratio > threshold) {
          String s =
              String.format(
                  "The average duration of a Cache load (%s ms) exceeds the threshold (%s ms)",
                  ratio, threshold);
          if (logger.isDebugEnabled()) {
            logger.debug(s);
          }
          status.add(okayHealth(s));
        }
      }
    }
  }

  /**
   * Checks to make sure that the cache hit ratio during the previous health check interval is less
   * than the {@linkplain CacheHealthConfig#getMinHitRatio threshold}. If not, the status is "okay"
   * health.
   *
   * <P>
   *
   * The following formula is used to compute the hit ratio:
   *
   * <PRE>
   * hitRatio = (gets - (loadsCompleted + netsearchesCompleted)) / (gets)
   * </PRE>
   *
   *
   * @see CachePerfStats#getGets
   * @see CachePerfStats#getLoadsCompleted
   * @see CachePerfStats#getNetsearchesCompleted
   */
  private void checkHitRatio(List<HealthStatus> status) {
    if (this.cacheStats == null || isFirstEvaluation() || this.cacheStats.isClosed()) {
      return;
    }

    long deltaGets = this.cacheStats.getGets() - this.prevGets;
    if (deltaGets != 0) {
      long deltaLoadsCompleted = this.cacheStats.getLoadsCompleted() - this.prevLoadsCompleted;
      long deltaNetsearchesCompleted =
          this.cacheStats.getNetsearchesCompleted() - this.prevNetsearchesCompleted;

      double hits = deltaGets - (deltaLoadsCompleted + deltaNetsearchesCompleted);
      double hitRatio = hits / deltaGets;
      double threshold = this.config.getMinHitRatio();
      if (hitRatio < threshold) {
        String s = "The hit ratio of this Cache (" + hitRatio + ") is below the threshold ("
            + threshold + ')';
        status.add(okayHealth(s));
      }
    }
  }

  /**
   * Checks to make sure that the {@linkplain CachePerfStats#getEventQueueSize cache event queue
   * size} does not exceed the {@linkplain CacheHealthConfig#getMaxEventQueueSize threshold}. If it
   * does, the status is "okay" health.
   */
  private void checkEventQueueSize(List<HealthStatus> status) {
    if (this.cacheStats == null || isFirstEvaluation() || this.cacheStats.isClosed()) {
      return;
    }

    long eventQueueSize = this.cacheStats.getEventQueueSize();
    long threshold = this.config.getMaxEventQueueSize();
    if (eventQueueSize > threshold) {
      String s =
          String.format("The size of the cache event queue (%s ms) exceeds the threshold (%s ms)",
              eventQueueSize, threshold);
      status.add(okayHealth(s));
    }
  }

  /**
   * Updates the previous values of statistics
   */
  private void updatePrevious() {
    if (this.cacheStats != null && !this.cacheStats.isClosed()) {
      this.prevLoadTime = this.cacheStats.getLoadTime();
      this.prevLoadsCompleted = this.cacheStats.getLoadsCompleted();
      this.prevNetsearchTime = this.cacheStats.getNetsearchTime();
      this.prevNetsearchesCompleted = this.cacheStats.getNetsearchesCompleted();
      this.prevGets = this.cacheStats.getGets();

    } else {
      this.prevLoadTime = 0L;
      this.prevLoadsCompleted = 0L;
      this.prevNetsearchTime = 0L;
      this.prevNetsearchesCompleted = 0L;
      this.prevGets = 0L;
    }
  }

  @Override
  protected void check(List status) {
    checkNetSearchTime(status);
    checkLoadTime(status);
    checkHitRatio(status);
    checkEventQueueSize(status);

    updatePrevious();
  }

  @Override
  public void close() {
    GemFireCacheImpl.removeCacheLifecycleListener(this);
  }

  @Override
  public void cacheClosed(InternalCache cache) {
    // do nothing
  }
}
