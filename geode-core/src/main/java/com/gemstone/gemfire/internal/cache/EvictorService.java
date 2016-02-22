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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.EvictionCriteria;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.gemstone.gemfire.internal.offheap.Releasable;
/**
 * Schedules each iteration periodically. EvictorService takes absolute time and
 * a period as input and schedules Eviction at absolute times by calculating the
 * interval. For scheduling the next eviction iteration it also takes into
 * account the time taken to complete one iteration. If an iteration takes more
 * time than the specified period then another iteration is scheduled
 * immediately.
 * 
 * @author skumar
 * 
 */

public class EvictorService extends AbstractScheduledService {

  private final EvictionCriteria<Object, Object> criteria;

  // period is always in seconds
  private long interval;

  private volatile boolean stopScheduling;

  private long nextScheduleTime;

  private GemFireCacheImpl cache;

  private Region region;
  
  private volatile ScheduledExecutorService executorService;

  public EvictorService(EvictionCriteria<Object, Object> criteria,
      long evictorStartTime, long evictorInterval, TimeUnit unit, Region r) {
    this.criteria = criteria;
    this.interval = unit.toSeconds(evictorInterval);
    this.region = r;
    try {
      this.cache = GemFireCacheImpl.getExisting();
    } catch (CacheClosedException cce) {
      
    }
    //TODO: Unless we revisit System.currentTimeMillis or cacheTimeMillis keep the default
//    long now = (evictorStartTime != 0 ? evictorStartTime
//        + this.cache.getDistributionManager().getCacheTimeOffset() : this.cache
//        .getDistributionManager().cacheTimeMillis()) / 1000;
    long now = this.cache.getDistributionManager().cacheTimeMillis() / 1000;
    if (this.cache.getLoggerI18n().fineEnabled()) {
      this.cache.getLoggerI18n().fine(
          "EvictorService: The startTime(now) is " + now + " evictorStartTime : " + evictorStartTime);
    }
    
    this.nextScheduleTime = now + 10;

    if (this.cache.getLoggerI18n().fineEnabled()) {
      this.cache.getLoggerI18n().fine(
          "EvictorService: The startTime is " + this.nextScheduleTime);
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (this.cache.getLoggerI18n().fineEnabled()) {
      this.cache.getLoggerI18n()
          .fine(
              "EvictorService: Running the iteration at "
                  + cache.cacheTimeMillis());
    }
    if (stopScheduling || checkCancelled(cache)) {
      stopScheduling(); // if check cancelled
      if (this.cache.getLoggerI18n().fineEnabled()) {
        this.cache
            .getLoggerI18n()
            .fine(
                "EvictorService: Abort eviction since stopScheduling OR cancel in progress. Evicted 0 entries ");
      }
      return;
    }
    CachePerfStats stats = ((LocalRegion)this.region).getCachePerfStats();
    long startEvictionTime = stats.startCustomEviction();
    int evicted = 0;
    long startEvaluationTime = stats.startEvaluation();
    Iterator<Entry<Object, Object>> keysItr = null;
    long totalIterationsTime = 0;
    
    keysItr = this.criteria.getKeysToBeEvicted(this.cache
        .getDistributionManager().cacheTimeMillis(), this.region);
    try {
    stats.incEvaluations(this.region.size());
    // if we have been asked to stop scheduling
    // or the cache is closing stop in between.
    
    
    while (keysItr.hasNext() && !stopScheduling && !checkCancelled(cache)) {
      Map.Entry<Object, Object> entry = keysItr.next();
      long startIterationTime = this.cache
          .getDistributionManager().cacheTimeMillis();
      Object routingObj = entry.getValue();
      if (this.cache.getLoggerI18n().fineEnabled()) {
        this.cache.getLoggerI18n().fine(
            "EvictorService: Going to evict the following entry " + entry);
      }
      if (this.region instanceof PartitionedRegion) {
        try {
          PartitionedRegion pr = (PartitionedRegion)this.region;
          stats.incEvictionsInProgress();
          int bucketId = PartitionedRegionHelper.getHashKey(pr, routingObj);
          BucketRegion br = pr.getDataStore().getLocalBucketById(bucketId);
          // This has to be called on BucketRegion directly and not on the PR as
          // PR doesn't allow operation on Secondary buckets.
          if (br != null) {
            if (this.cache.getLoggerI18n().fineEnabled()) {
              this.cache.getLoggerI18n().fine(
                  "EvictorService: Going to evict the following entry " + entry
                      + " from bucket " + br);
            }
            if (br.getBucketAdvisor().isPrimary()) {
              boolean succ = false;
              try {
                succ = br.customEvictDestroy(entry.getKey());
              } catch (PrimaryBucketException e) {
                if (this.cache.getLoggerI18n().fineEnabled()) {
                  this.cache.getLoggerI18n().warning(
                      LocalizedStrings.EVICTORSERVICE_CAUGHT_EXCEPTION_0, e);
                }
              }
              
              if (succ)
                evicted++;
              if (this.cache.getLoggerI18n().fineEnabled()) {
                this.cache.getLoggerI18n()
                    .fine(
                        "EvictorService: Evicted the following entry " + entry
                            + " from bucket " + br + " successfully " + succ
                            + " the value in buk is " /*
                                                       * +
                                                       * br.get(entry.getKey())
                                                       */);
              }
            }
          }
          stats.incEvictions();
        } catch (Exception e) {
          if (this.cache.getLoggerI18n().fineEnabled()) {
            this.cache.getLoggerI18n().warning(
                LocalizedStrings.EVICTORSERVICE_CAUGHT_EXCEPTION_0, e);
          }
          // TODO:
          // Do the exception handling .
          // Check if the bucket is present
          // If the entry could not be evicted then log the warning
          // Log any other exception.
        }finally{
          stats.decEvictionsInProgress();
          long endIterationTime = this.cache
              .getDistributionManager().cacheTimeMillis();
          totalIterationsTime += (endIterationTime - startIterationTime);
        }
      }
    }
    }finally {
      if(keysItr instanceof Releasable) {
        ((Releasable)keysItr).release();
      }
    }
    stats.endEvaluation(startEvaluationTime, totalIterationsTime);    
    
    if (this.cache.getLoggerI18n().fineEnabled()) {
      this.cache.getLoggerI18n().fine(
          "EvictorService: Completed an iteration at time "
              + this.cache.getDistributionManager().cacheTimeMillis() / 1000
              + ". Evicted " + evicted + " entries.");
    }
    stats.endCustomEviction(startEvictionTime);
  }

  private boolean checkCancelled(GemFireCacheImpl cache) {
    if (cache.getCancelCriterion().cancelInProgress() != null) {
      return true;
    }
    return false;
  }

  @Override
  protected Scheduler scheduler() {
    return new CustomScheduler() {
      @Override
      protected Schedule getNextSchedule() throws Exception {
        // get the current time in seconds from DM.
        // it takes care of clock skew etc in different VMs
        long now = cache.getDistributionManager().cacheTimeMillis() / 1000;
        if (cache.getLoggerI18n().fineEnabled()) {
          cache.getLoggerI18n().fine("EvictorService: Now is " + now);
        }
        long delay = 0;
        if (now < nextScheduleTime) {
          delay = nextScheduleTime - now;
        }
        nextScheduleTime += interval;
        // calculate the next immediate time i.e. schedule time in seconds
        // set the schedule.delay to that scheduletime - currenttime
        if (cache.getLoggerI18n().fineEnabled()) {
          cache.getLoggerI18n().fine(
              "EvictorService: Returning the next schedule with delay " + delay
                  + " next schedule is at : " + nextScheduleTime);
        }

        return new Schedule(delay, TimeUnit.SECONDS);
      }
    };
  }

  /**
   * Region.destroy and Region.close should make sure to call this method. This
   * will be called here.
   */
  public void stopScheduling() {
    this.stopScheduling = true;
  }

  // this will be called when we stop the service.
  // not sure if we have to do any cleanup
  // to stop the service call stop()
  @Override
  protected void shutDown() throws Exception {
    this.executorService.shutdownNow();
    this.region= null;
    this.cache = null;
  }

  // This will be called when we start the service.
  // not sure if we have to any intialization
  @Override
  protected void startUp() throws Exception {

  }

  public void changeEvictionInterval(long newInterval) {
    this.interval = newInterval / 1000;
    if (cache.getLoggerI18n().fineEnabled()) {
      cache.getLoggerI18n().fine(
          "EvictorService: New interval is " + this.interval);
    }
  }

  public void changeStartTime(long newStart) {
    this.nextScheduleTime = newStart/1000;
    if (cache.getLoggerI18n().fineEnabled()) {
      cache.getLoggerI18n().fine("EvictorService: New start time is " + this.nextScheduleTime);
    }
  }
  
  protected ScheduledExecutorService executor() {
    this.executorService = super.executor();
    return this.executorService;
  }

}
