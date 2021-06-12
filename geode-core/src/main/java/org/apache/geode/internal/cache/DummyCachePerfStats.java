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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.PoolStatHelper;

/**
 * @since GemFire 5.7
 */
public class DummyCachePerfStats extends CachePerfStats {

  DummyCachePerfStats() {
    super(null, disabledClock());
  }

  @Override
  public long getLoadsCompleted() {
    return 0L;
  }

  @Override
  public long getLoadTime() {
    return 0L;
  }

  @Override
  public long getNetloadsCompleted() {
    return 0L;
  }

  @Override
  public long getNetsearchesCompleted() {
    return 0L;
  }

  @Override
  public long getNetsearchTime() {
    return 0L;
  }

  @Override
  public long getGetInitialImagesCompleted() {
    return 0L;
  }

  @Override
  public long getGetInitialImageKeysReceived() {
    return 0L;
  }

  @Override
  public long getRegions() {
    return 0L;
  }

  @Override
  public long getDestroys() {
    return 0L;
  }

  @Override
  public long getCreates() {
    return 0L;
  }

  @Override
  public long getPuts() {
    return 0L;
  }

  @Override
  public long getPutAlls() {
    return 0L;
  }

  @Override
  public long getUpdates() {
    return 0L;
  }

  @Override
  public long getInvalidates() {
    return 0L;
  }

  @Override
  public long getGets() {
    return 0L;
  }

  @Override
  public long getMisses() {
    return 0L;
  }

  @Override
  public long getReliableQueuedOps() {
    return 0L;
  }

  @Override
  public void incReliableQueuedOps(long inc) {}

  @Override
  public void incReliableQueueSize(long inc) {}

  @Override
  public void incReliableQueueMax(long inc) {}

  @Override
  public void incReliableRegions(long inc) {}

  @Override
  public long getReliableRegionsMissing() {
    return 0L;
  }

  @Override
  public void incReliableRegionsMissing(long inc) {}

  @Override
  public void incReliableRegionsQueuing(long inc) {}

  @Override
  public long getReliableRegionsMissingFullAccess() {
    return 0L;
  }

  @Override
  public void incReliableRegionsMissingFullAccess(long inc) {}

  @Override
  public long getReliableRegionsMissingLimitedAccess() {
    return 0L;
  }

  @Override
  public void incReliableRegionsMissingLimitedAccess(long inc) {}

  @Override
  public long getReliableRegionsMissingNoAccess() {
    return 0L;
  }

  @Override
  public void incReliableRegionsMissingNoAccess(long inc) {}

  @Override
  public void incQueuedEvents(long inc) {}

  // //////////////////// Updating Stats //////////////////////

  @Override
  public long startLoad() {
    return 0L;
  }

  @Override
  public void endLoad(long start) {}

  @Override
  public long startNetload() {
    return 0L;
  }

  @Override
  public void endNetload(long start) {}

  @Override
  public long startNetsearch() {
    return 0L;
  }

  @Override
  public void endNetsearch(long start) {}

  @Override
  public long startCacheWriterCall() {
    return 0L;
  }

  @Override
  public void endCacheWriterCall(long start) {}

  @Override
  public long startCacheListenerCall() {
    return 0L;
  }

  @Override
  public void endCacheListenerCall(long start) {}

  @Override
  public long startGetInitialImage() {
    return 0L;
  }

  @Override
  public void endGetInitialImage(long start) {}

  @Override
  public void endNoGIIDone(long start) {}

  @Override
  public void incGetInitialImageKeysReceived() {}

  @Override
  public void incRegions(long inc) {}

  @Override
  public void incPartitionedRegions(long inc) {}

  @Override
  public void incDestroys() {}

  @Override
  public void incCreates() {}

  @Override
  public void incInvalidates() {}

  @Override
  public long startGet() {
    return 0L;
  }

  @Override
  public void endGet(long start, boolean miss) {}

  @Override
  public long endPut(long start, boolean isUpdate) {
    return 0L;
  }

  @Override
  public void endPutAll(long start) {}

  @Override
  public void endQueryExecution(long executionTime) {}

  @Override
  public long getTxCommits() {
    return 0L;
  }

  @Override
  public long getTxCommitChanges() {
    return 0L;
  }

  @Override
  public long getTxCommitTime() {
    return 0L;
  }

  @Override
  public long getTxSuccessLifeTime() {
    return 0L;
  }

  @Override
  public long getTxFailures() {
    return 0L;
  }

  @Override
  public long getTxFailureChanges() {
    return 0L;
  }

  @Override
  public long getTxFailureTime() {
    return 0L;
  }

  @Override
  public long getTxFailedLifeTime() {
    return 0L;
  }

  @Override
  public long getTxRollbacks() {
    return 0L;
  }

  @Override
  public long getTxRollbackChanges() {
    return 0L;
  }

  @Override
  public long getTxRollbackTime() {
    return 0L;
  }

  @Override
  public long getTxRollbackLifeTime() {
    return 0L;
  }

  @Override
  public void incTxConflictCheckTime(long delta) {}

  @Override
  public void txSuccess(long opTime, long txLifeTime, long txChanges) {}

  @Override
  public void txFailure(long opTime, long txLifeTime, long txChanges) {}

  @Override
  public void txRollback(long opTime, long txLifeTime, long txChanges) {}

  @Override
  protected void close() {
    // nothing
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public long getEventQueueSize() {
    return 0L;
  }

  @Override
  public void incEventQueueSize(long items) {}

  @Override
  public void incEventQueueThrottleCount(long items) {}

  @Override
  protected void incEventQueueThrottleTime(long nanos) {}

  @Override
  protected void incEventThreads(long items) {}

  @Override
  public void incEntryCount(long delta) {}

  @Override
  public void incRetries() {}

  @Override
  public Statistics getStats() {
    return null;
  }

  @Override
  public PoolStatHelper getEventPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {}

      @Override
      public void endJob() {}
    };
  }

}
