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

import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.QueueStatHelper;

/**
 * Empty implementation of <code>DistributedLockStats</code> used when there is currently no
 * connection to the distributed system.
 *
 */
public class DummyDLockStats implements DistributedLockStats {

  public int getLockWaitsInProgress() {
    return -1;
  }

  public int getLockWaitsCompleted() {
    return -1;
  }

  public int getLockWaitsFailed() {
    return -1;
  }

  public long getLockWaitTime() {
    return -1;
  }

  public long getLockWaitFailedTime() {
    return -1;
  }

  public long startLockWait() {
    return -1;
  }

  public void endLockWait(long start, boolean success) {}

  public int getWaitingQueueSize() {
    return -1;
  }

  public void incWaitingQueueSize(int messages) {}

  public int getSerialQueueSize() {
    return -1;
  }

  public void incSerialQueueSize(int messages) {}

  public int getNumSerialThreads() {
    return -1;
  }

  public void incNumSerialThreads(int threads) {}

  public int getWaitingThreads() {
    return -1;
  }

  public void incWaitingThreads(int threads) {}

  public int getServices() {
    return -1;
  }

  public void incServices(int val) {}

  public int getGrantors() {
    return -1;
  }

  public void incGrantors(int val) {}

  public int getGrantWaitsInProgress() {
    return -1;
  }

  public int getGrantWaitsCompleted() {
    return -1;
  }

  public int getGrantWaitsFailed() {
    return -1;
  }

  public long getGrantWaitTime() {
    return -1;
  }

  public long getGrantWaitFailedTime() {
    return -1;
  }

  public long startGrantWait() {
    return -1;
  }

  public void endGrantWait(long start) {}

  public void endGrantWaitNotGrantor(long start) {}

  public void endGrantWaitTimeout(long start) {}

  public void endGrantWaitNotHolder(long start) {}

  public void endGrantWaitFailed(long start) {}

  public void endGrantWaitSuspended(long start) {}

  public void endGrantWaitDestroyed(long start) {}

  public int getCreateGrantorsInProgress() {
    return -1;
  }

  public int getCreateGrantorsCompleted() {
    return -1;
  }

  public long getCreateGrantorTime() {
    return -1;
  }

  public long startCreateGrantor() {
    return -1;
  }

  public void endCreateGrantor(long start) {}

  public int getServiceCreatesInProgress() {
    return -1;
  }

  public int getServiceCreatesCompleted() {
    return -1;
  }

  public long startServiceCreate() {
    return -1;
  }

  public void serviceCreateLatchReleased(long start) {}

  public void serviceInitLatchReleased(long start) {}

  public long getServiceCreateLatchTime() {
    return -1;
  }

  public long getServiceInitLatchTime() {
    return -1;
  }

  public int getGrantorWaitsInProgress() {
    return -1;
  }

  public int getGrantorWaitsCompleted() {
    return -1;
  }

  public int getGrantorWaitsFailed() {
    return -1;
  }

  public long getGrantorWaitTime() {
    return -1;
  }

  public long getGrantorWaitFailedTime() {
    return -1;
  }

  public long startGrantorWait() {
    return -1;
  }

  public void endGrantorWait(long start, boolean success) {}

  public QueueStatHelper getSerialQueueHelper() {
    return new DummyQueueStatHelper();
  }

  public PoolStatHelper getWaitingPoolHelper() {
    return new DummyPoolStatHelper();
  }

  public QueueStatHelper getWaitingQueueHelper() {
    return new DummyQueueStatHelper();
  }

  public int getGrantorThreadsInProgress() {
    return -1;
  }

  public int getGrantorThreadsCompleted() {
    return -1;
  }

  public long getGrantorThreadTime() {
    return -1;
  }

  public long getGrantorThreadExpireAndGrantLocksTime() {
    return -1;
  }

  public long getGrantorThreadHandleRequestTimeoutsTime() {
    return -1;
  }

  public long getGrantorThreadRemoveUnusedTokensTime() {
    return -1;
  }

  public long startGrantorThread() {
    return -1;
  }

  public long endGrantorThreadExpireAndGrantLocks(long start) {
    return -1;
  }

  public long endGrantorThreadHandleRequestTimeouts(long timing) {
    return -1;
  }

  public void endGrantorThreadRemoveUnusedTokens(long timing) {}

  public void endGrantorThread(long start) {}

  public int getPendingRequests() {
    return -1;
  }

  public void incPendingRequests(int val) {}

  public int getDestroyReadWaitsInProgress() {
    return -1;
  }

  public int getDestroyReadWaitsCompleted() {
    return -1;
  }

  public int getDestroyReadWaitsFailed() {
    return -1;
  }

  public long getDestroyReadWaitTime() {
    return -1;
  }

  public long getDestroyReadWaitFailedTime() {
    return -1;
  }

  public long startDestroyReadWait() {
    return -1;
  }

  public void endDestroyReadWait(long start, boolean success) {}

  public int getDestroyWriteWaitsInProgress() {
    return -1;
  }

  public int getDestroyWriteWaitsCompleted() {
    return -1;
  }

  public int getDestroyWriteWaitsFailed() {
    return -1;
  }

  public long getDestroyWriteWaitTime() {
    return -1;
  }

  public long getDestroyWriteWaitFailedTime() {
    return -1;
  }

  public long startDestroyWriteWait() {
    return -1;
  }

  public void endDestroyWriteWait(long start, boolean success) {}

  public int getDestroyReads() {
    return -1;
  }

  public void incDestroyReads(int val) {}

  public int getDestroyWrites() {
    return -1;
  }

  public void incDestroyWrites(int val) {}

  public int getLockReleasesInProgress() {
    return -1;
  }

  public int getLockReleasesCompleted() {
    return -1;
  }

  public long getLockReleaseTime() {
    return -1;
  }

  public long startLockRelease() {
    return -1;
  }

  public void endLockRelease(long start) {}

  public int getBecomeGrantorRequests() {
    return -1;
  }

  public void incBecomeGrantorRequests() {}

  public int getTokens() {
    return -1;
  }

  public void incTokens(int val) {}

  public int getGrantTokens() {
    return -1;
  }

  public void incGrantTokens(int val) {}

  public int getRequestQueues() {
    return -1;
  }

  public void incRequestQueues(int val) {}

  public int getFreeResourcesCompleted() {
    return -1;
  }

  public void incFreeResourcesCompleted() {}

  public int getFreeResourcesFailed() {
    return -1;
  }

  public void incFreeResourcesFailed() {}

  public static class DummyPoolStatHelper implements PoolStatHelper {
    public void startJob() {}

    public void endJob() {}
  }

  public static class DummyQueueStatHelper implements QueueStatHelper {
    public void add() {}

    public void remove() {}

    public void remove(int count) {}
  }

}
