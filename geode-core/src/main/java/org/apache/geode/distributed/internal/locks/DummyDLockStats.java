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
  public int getLockWaitsInProgress() {
    return -1;
  }

  @Override
  public int getLockWaitsCompleted() {
    return -1;
  }

  @Override
  public int getLockWaitsFailed() {
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
  public int getWaitingQueueSize() {
    return -1;
  }

  @Override
  public void incWaitingQueueSize(int messages) {}

  @Override
  public int getSerialQueueSize() {
    return -1;
  }

  @Override
  public void incSerialQueueSize(int messages) {}

  @Override
  public int getNumSerialThreads() {
    return -1;
  }

  @Override
  public void incNumSerialThreads(int threads) {}

  @Override
  public int getWaitingThreads() {
    return -1;
  }

  @Override
  public void incWaitingThreads(int threads) {}

  @Override
  public int getServices() {
    return -1;
  }

  @Override
  public void incServices(int val) {}

  @Override
  public int getGrantors() {
    return -1;
  }

  @Override
  public void incGrantors(int val) {}

  @Override
  public int getGrantWaitsInProgress() {
    return -1;
  }

  @Override
  public int getGrantWaitsCompleted() {
    return -1;
  }

  @Override
  public int getGrantWaitsFailed() {
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
  public int getCreateGrantorsInProgress() {
    return -1;
  }

  @Override
  public int getCreateGrantorsCompleted() {
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
  public int getServiceCreatesInProgress() {
    return -1;
  }

  @Override
  public int getServiceCreatesCompleted() {
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
  public int getGrantorWaitsInProgress() {
    return -1;
  }

  @Override
  public int getGrantorWaitsCompleted() {
    return -1;
  }

  @Override
  public int getGrantorWaitsFailed() {
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
  public int getGrantorThreadsInProgress() {
    return -1;
  }

  @Override
  public int getGrantorThreadsCompleted() {
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
  public int getPendingRequests() {
    return -1;
  }

  @Override
  public void incPendingRequests(int val) {}

  @Override
  public int getDestroyReadWaitsInProgress() {
    return -1;
  }

  @Override
  public int getDestroyReadWaitsCompleted() {
    return -1;
  }

  @Override
  public int getDestroyReadWaitsFailed() {
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
  public int getDestroyWriteWaitsInProgress() {
    return -1;
  }

  @Override
  public int getDestroyWriteWaitsCompleted() {
    return -1;
  }

  @Override
  public int getDestroyWriteWaitsFailed() {
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
  public int getDestroyReads() {
    return -1;
  }

  @Override
  public void incDestroyReads(int val) {}

  @Override
  public int getDestroyWrites() {
    return -1;
  }

  @Override
  public void incDestroyWrites(int val) {}

  @Override
  public int getLockReleasesInProgress() {
    return -1;
  }

  @Override
  public int getLockReleasesCompleted() {
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
  public int getBecomeGrantorRequests() {
    return -1;
  }

  @Override
  public void incBecomeGrantorRequests() {}

  @Override
  public int getTokens() {
    return -1;
  }

  @Override
  public void incTokens(int val) {}

  @Override
  public int getGrantTokens() {
    return -1;
  }

  @Override
  public void incGrantTokens(int val) {}

  @Override
  public int getRequestQueues() {
    return -1;
  }

  @Override
  public void incRequestQueues(int val) {}

  @Override
  public int getFreeResourcesCompleted() {
    return -1;
  }

  @Override
  public void incFreeResourcesCompleted() {}

  @Override
  public int getFreeResourcesFailed() {
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
    public void remove(int count) {}
  }

}
