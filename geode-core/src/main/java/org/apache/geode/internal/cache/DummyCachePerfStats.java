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
package org.apache.geode.internal.cache;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.PoolStatHelper;

/**
 * @since GemFire 5.7
 */
public class DummyCachePerfStats extends CachePerfStats {

  // ////////////////////// Constructors ////////////////////////

  /**
   * Creates a new <code>DummyCachePerfStats</code>
   */
  public DummyCachePerfStats() {
  }

  // //////////////////// Accessing Stats //////////////////////

  @Override
  public int getLoadsInProgress() {
    return 0;
  }

  @Override
  public int getLoadsCompleted() {
    return 0;
  }

  @Override
  public long getLoadTime() {
    return 0;
  }

  @Override
  public int getNetloadsInProgress() {
    return 0;
  }

  @Override
  public int getNetloadsCompleted() {
    return 0;
  }

  @Override
  public long getNetloadTime() {
    return 0;
  }

  @Override
  public int getNetsearchesInProgress() {
    return 0;
  }

  @Override
  public int getNetsearchesCompleted() {
    return 0;
  }

  @Override
  public long getNetsearchTime() {
    return 0;
  }

  @Override
  public int getGetInitialImagesInProgress() {
    return 0;
  }

  @Override
  public int getGetInitialImagesCompleted() {
    return 0;
  }

  @Override
  public long getGetInitialImageTime() {
    return 0;
  }

  @Override
  public int getGetInitialImageKeysReceived() {
    return 0;
  }

  @Override
  public int getRegions() {
    return 0;
  }

  @Override
  public int getPartitionedRegions() {
    return 0;
  }

  @Override
  public int getDestroys() {
    return 0;
  }

  @Override
  public int getCreates() {
    return 0;
  }

  @Override
  public int getPuts() {
    return 0;
  }

  @Override
  public int getPutAlls() {
    return 0;
  }

  @Override
  public int getUpdates() {
    return 0;
  }

  @Override
  public int getInvalidates() {
    return 0;
  }

  @Override
  public int getGets() {
    return 0;
  }

  @Override
  public int getMisses() {
    return 0;
  }

  @Override
  public int getReliableQueuedOps() {
    return 0;
  }

  @Override
  public void incReliableQueuedOps(int inc) {
  }

  @Override
  public int getReliableQueueSize() {
    return 0;
  }

  @Override
  public void incReliableQueueSize(int inc) {
  }

  @Override
  public int getReliableQueueMax() {
    return 0;
  }

  @Override
  public void incReliableQueueMax(int inc) {
  }

  @Override
  public int getReliableRegions() {
    return 0;
  }

  @Override
  public void incReliableRegions(int inc) {
  }

  @Override
  public int getReliableRegionsMissing() {
    return 0;
  }

  @Override
  public void incReliableRegionsMissing(int inc) {
  }

  @Override
  public int getReliableRegionsQueuing() {
    return 0;
  }

  @Override
  public void incReliableRegionsQueuing(int inc) {
  }

  @Override
  public int getReliableRegionsMissingFullAccess() {
    return 0;
  }

  @Override
  public void incReliableRegionsMissingFullAccess(int inc) {
  }

  @Override
  public int getReliableRegionsMissingLimitedAccess() {
    return 0;
  }

  @Override
  public void incReliableRegionsMissingLimitedAccess(int inc) {
  }

  @Override
  public int getReliableRegionsMissingNoAccess() {
    return 0;
  }

  @Override
  public void incReliableRegionsMissingNoAccess(int inc) {
  }

  @Override
  public void incQueuedEvents(int inc) {
  }

  @Override
  public long getQueuedEvents() {
    return 0;
  }

  // //////////////////// Updating Stats //////////////////////

  @Override
  public long startLoad() {
    return 0;
  }

  @Override
  public void endLoad(long start) {
  }

  @Override
  public long startNetload() {
    return 0;
  }

  @Override
  public void endNetload(long start) {
  }

  @Override
  public long startNetsearch() {
    return 0;
  }

  @Override
  public void endNetsearch(long start) {
  }

  @Override
  public long startCacheWriterCall() {
    return 0;
  }

  @Override
  public void endCacheWriterCall(long start) {
  }

  @Override
  public long startCacheListenerCall() {
    return 0;
  }

  @Override
  public void endCacheListenerCall(long start) {}

  @Override
  public long startGetInitialImage() {
    return 0;
  }

  @Override
  public void endGetInitialImage(long start) {
  }

  @Override
  public void endNoGIIDone(long start) {
  }

  @Override
  public void incGetInitialImageKeysReceived() {
  }

  @Override
  public void incRegions(int inc) {
  }

  @Override
  public void incPartitionedRegions(int inc) {
  }

  @Override
  public void incDestroys() {
  }

  @Override
  public void incCreates() {
  }

  @Override
  public void incInvalidates() {
  }

  @Override
  public long startGet() {
    return 0;
  }

  @Override
  public void endGet(long start, boolean miss) {
  }

  @Override
  public long endPut(long start, boolean isUpdate) {
    return 0;
  }

  @Override
  public void endPutAll(long start) {
  }

  @Override
  public void endQueryExecution(long executionTime) {
  }

  @Override
  public int getTxCommits() {
    return 0;
  }

  @Override
  public int getTxCommitChanges() {
    return 0;
  }

  @Override
  public long getTxCommitTime() {
    return 0;
  }

  @Override
  public long getTxSuccessLifeTime() {
    return 0;
  }

  @Override
  public int getTxFailures() {
    return 0;
  }

  @Override
  public int getTxFailureChanges() {
    return 0;
  }

  @Override
  public long getTxFailureTime() {
    return 0;
  }

  @Override
 public long getTxFailedLifeTime() {
    return 0;
  }

  @Override
  public int getTxRollbacks() {
    return 0;
  }

  @Override
  public int getTxRollbackChanges() {
    return 0;
  }

  @Override
  public long getTxRollbackTime() {
    return 0;
  }

  @Override
  public long getTxRollbackLifeTime() {
    return 0;
  }

  @Override
  public void incTxConflictCheckTime(long delta) {
  }

  @Override
  public void txSuccess(long opTime, long txLifeTime, int txChanges) {
  }

  @Override
  public void txFailure(long opTime, long txLifeTime, int txChanges) {
  }

  @Override
  public void txRollback(long opTime, long txLifeTime, int txChanges) {
  }

  // //// Special Instance Methods /////

  @Override
  void close() {
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public int getEventQueueSize() {
    return 0;
  }

  @Override
  public void incEventQueueSize(int items) {
  }

  @Override
  public void incEventQueueThrottleCount(int items) {
  }

  @Override
  protected void incEventQueueThrottleTime(long nanos) {
  }

  @Override
  protected void incEventThreads(int items) {
  }

  @Override
  public void incEntryCount(int delta) {
  }

  @Override
  public long getEntries() {
    return 0;
  }

  @Override
  public void incRetries() {
  }

  @Override
  public Statistics getStats() {
    return null;
  }

  @Override
  public PoolStatHelper getEventPoolHelper() {
    return new PoolStatHelper() {
      public void startJob() {
      }

      public void endJob() {
      }
    };
  }

}
