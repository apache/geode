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

package org.apache.geode.distributed.internal.locks;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.QueueStatHelper;

/**
 * Empty implementation of <code>DistributedLockStats</code> used when there is currently no
 * connection to the distributed system.
 *
 */
@Immutable
public class DummyDLockStats implements DistributedLockStats {

  @Override
  public long getLockWaitsInProgress() {
    return -1;
  }

  @Override
  public long getLockWaitsCompleted() {
    return -1;
  }

  @Override
  public long getLockWaitsFailed() {
    return -1;
  }

  @Override
  public long getLockWaitTime() {
    return -1;
  }

  @Override
  public long getLockWaitFailedTime() {
    return -1;
  }

  @Override
  public long startLockWait() {
    return -1;
  }

  @Override
  public void endLockWait(long start, boolean success) {}

  @Override
  public long getWaitingQueueSize() {
    return -1;
  }

  @Override
  public void incWaitingQueueSize(long messages) {}

  @Override
  public long getSerialQueueSize() {
    return -1;
  }

  @Override
  public void incSerialQueueSize(long messages) {}

  @Override
  public long getNumSerialThreads() {
    return -1;
  }

  @Override
  public void incNumSerialThreads(long threads) {}

  @Override
  public long getWaitingThreads() {
    return -1;
  }

  @Override
  public void incWaitingThreads(long threads) {}

  @Override
  public long getServices() {
    return -1;
  }

  @Override
  public void incServices(long val) {}

  @Override
  public long getGrantors() {
    return -1;
  }

  @Override
  public void incGrantors(long val) {}

  @Override
  public long getGrantWaitsInProgress() {
    return -1;
  }

  @Override
  public long getGrantWaitsCompleted() {
    return -1;
  }

  @Override
  public long getGrantWaitsFailed() {
    return -1;
  }

  @Override
  public long getGrantWaitTime() {
    return -1;
  }

  @Override
  public long getGrantWaitFailedTime() {
    return -1;
  }

  @Override
  public long startGrantWait() {
    return -1;
  }

  @Override
  public void endGrantWait(long start) {}

  @Override
  public void endGrantWaitNotGrantor(long start) {}

  @Override
  public void endGrantWaitTimeout(long start) {}

  @Override
  public void endGrantWaitNotHolder(long start) {}

  @Override
  public void endGrantWaitFailed(long start) {}

  @Override
  public void endGrantWaitSuspended(long start) {}

  @Override
  public void endGrantWaitDestroyed(long start) {}

  @Override
  public long getCreateGrantorsInProgress() {
    return -1;
  }

  @Override
  public long getCreateGrantorsCompleted() {
    return -1;
  }

  @Override
  public long getCreateGrantorTime() {
    return -1;
  }

  @Override
  public long startCreateGrantor() {
    return -1;
  }

  @Override
  public void endCreateGrantor(long start) {}

  @Override
  public long getServiceCreatesInProgress() {
    return -1;
  }

  @Override
  public long getServiceCreatesCompleted() {
    return -1;
  }

  @Override
  public long startServiceCreate() {
    return -1;
  }

  @Override
  public void serviceCreateLatchReleased(long start) {}

  @Override
  public void serviceInitLatchReleased(long start) {}

  @Override
  public long getServiceCreateLatchTime() {
    return -1;
  }

  @Override
  public long getServiceInitLatchTime() {
    return -1;
  }

  @Override
  public long getGrantorWaitsInProgress() {
    return -1;
  }

  @Override
  public long getGrantorWaitsCompleted() {
    return -1;
  }

  @Override
  public long getGrantorWaitsFailed() {
    return -1;
  }

  @Override
  public long getGrantorWaitTime() {
    return -1;
  }

  @Override
  public long getGrantorWaitFailedTime() {
    return -1;
  }

  @Override
  public long startGrantorWait() {
    return -1;
  }

  @Override
  public void endGrantorWait(long start, boolean success) {}

  @Override
  public QueueStatHelper getSerialQueueHelper() {
    return new DummyQueueStatHelper();
  }

  @Override
  public PoolStatHelper getWaitingPoolHelper() {
    return new DummyPoolStatHelper();
  }

  @Override
  public QueueStatHelper getWaitingQueueHelper() {
    return new DummyQueueStatHelper();
  }

  @Override
  public long getGrantorThreadsInProgress() {
    return -1;
  }

  @Override
  public long getGrantorThreadsCompleted() {
    return -1;
  }

  @Override
  public long getGrantorThreadTime() {
    return -1;
  }

  @Override
  public long getGrantorThreadExpireAndGrantLocksTime() {
    return -1;
  }

  @Override
  public long getGrantorThreadHandleRequestTimeoutsTime() {
    return -1;
  }

  @Override
  public long getGrantorThreadRemoveUnusedTokensTime() {
    return -1;
  }

  @Override
  public long startGrantorThread() {
    return -1;
  }

  @Override
  public long endGrantorThreadExpireAndGrantLocks(long start) {
    return -1;
  }

  @Override
  public long endGrantorThreadHandleRequestTimeouts(long timing) {
    return -1;
  }

  @Override
  public void endGrantorThreadRemoveUnusedTokens(long timing) {}

  @Override
  public void endGrantorThread(long start) {}

  @Override
  public long getPendingRequests() {
    return -1;
  }

  @Override
  public void incPendingRequests(long val) {}

  @Override
  public long getDestroyReadWaitsInProgress() {
    return -1;
  }

  @Override
  public long getDestroyReadWaitsCompleted() {
    return -1;
  }

  @Override
  public long getDestroyReadWaitsFailed() {
    return -1;
  }

  @Override
  public long getDestroyReadWaitTime() {
    return -1;
  }

  @Override
  public long getDestroyReadWaitFailedTime() {
    return -1;
  }

  @Override
  public long startDestroyReadWait() {
    return -1;
  }

  @Override
  public void endDestroyReadWait(long start, boolean success) {}

  @Override
  public long getDestroyWriteWaitsInProgress() {
    return -1;
  }

  @Override
  public long getDestroyWriteWaitsCompleted() {
    return -1;
  }

  @Override
  public long getDestroyWriteWaitsFailed() {
    return -1;
  }

  @Override
  public long getDestroyWriteWaitTime() {
    return -1;
  }

  @Override
  public long getDestroyWriteWaitFailedTime() {
    return -1;
  }

  @Override
  public long startDestroyWriteWait() {
    return -1;
  }

  @Override
  public void endDestroyWriteWait(long start, boolean success) {}

  @Override
  public long getDestroyReads() {
    return -1;
  }

  @Override
  public void incDestroyReads(long val) {}

  @Override
  public long getDestroyWrites() {
    return -1;
  }

  @Override
  public void incDestroyWrites(long val) {}

  @Override
  public long getLockReleasesInProgress() {
    return -1;
  }

  @Override
  public long getLockReleasesCompleted() {
    return -1;
  }

  @Override
  public long getLockReleaseTime() {
    return -1;
  }

  @Override
  public long startLockRelease() {
    return -1;
  }

  @Override
  public void endLockRelease(long start) {}

  @Override
  public long getBecomeGrantorRequests() {
    return -1;
  }

  @Override
  public void incBecomeGrantorRequests() {}

  @Override
  public long getTokens() {
    return -1;
  }

  @Override
  public void incTokens(long val) {}

  @Override
  public long getGrantTokens() {
    return -1;
  }

  @Override
  public void incGrantTokens(long val) {}

  @Override
  public long getRequestQueues() {
    return -1;
  }

  @Override
  public void incRequestQueues(long val) {}

  @Override
  public long getFreeResourcesCompleted() {
    return -1;
  }

  @Override
  public void incFreeResourcesCompleted() {}

  @Override
  public long getFreeResourcesFailed() {
    return -1;
  }

  @Override
  public void incFreeResourcesFailed() {}

  public static class DummyPoolStatHelper implements PoolStatHelper {
    @Override
    public void startJob() {}

    @Override
    public void endJob() {}
  }

  public static class DummyQueueStatHelper implements QueueStatHelper {
    @Override
    public void add() {}

    @Override
    public void remove() {}

    @Override
    public void remove(long count) {}
  }

}
