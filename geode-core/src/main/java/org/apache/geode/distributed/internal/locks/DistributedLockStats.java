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
 * Defines the interface used to access and modify distributed lock statistics.
 *
 *
 */
public interface DistributedLockStats {

  // time for call to lock() to complete

  /**
   * Returns the number of threads currently waiting for a distributed lock
   */
  long getLockWaitsInProgress();

  /**
   * Returns the total number of waits for a distributed lock
   */
  long getLockWaitsCompleted();

  long getLockWaitsFailed();

  /**
   * Returns the total number of nanoseconds spent waiting for a distributed lock.
   */
  long getLockWaitTime();

  long getLockWaitFailedTime();

  /**
   * @return the timestamp that marks the start of the operation
   */
  long startLockWait();

  /**
   * @param start the timestamp taken when the operation started
   */
  void endLockWait(long start, boolean success);

  // incSerialQueueSize everytime getWaitingQueueHelper add/remove called
  long getWaitingQueueSize();

  void incWaitingQueueSize(long messages);

  // incSerialQueueSize everytime getSerialQueueHelper add/remove called
  long getSerialQueueSize();

  void incSerialQueueSize(long messages);

  // incNumSerialThreads everytime we execute with dlock getSerialExecutor()
  long getNumSerialThreads();

  void incNumSerialThreads(long threads);

  // incWaitingThreads for every invoke of getWaitingPoolHelper startJob/endJob
  long getWaitingThreads();

  void incWaitingThreads(long threads);

  // current number of lock services used by this system member
  long getServices();

  void incServices(long val);

  // current number of lock grantors hosted by this system member
  long getGrantors();

  void incGrantors(long val);

  // time spent granting of lock requests to completion
  long getGrantWaitsInProgress();

  long getGrantWaitsCompleted();

  long getGrantWaitsFailed();

  long getGrantWaitTime();

  long getGrantWaitFailedTime();

  long startGrantWait();

  void endGrantWait(long start);

  void endGrantWaitNotGrantor(long start);

  void endGrantWaitTimeout(long start);

  void endGrantWaitNotHolder(long start);

  void endGrantWaitFailed(long start);

  void endGrantWaitDestroyed(long start);

  void endGrantWaitSuspended(long start);

  // time spent creating initial grantor for lock service
  long getCreateGrantorsInProgress();

  long getCreateGrantorsCompleted();

  long getCreateGrantorTime();

  long startCreateGrantor();

  void endCreateGrantor(long start);

  // time spent creating each lock service
  long getServiceCreatesInProgress();

  long getServiceCreatesCompleted();

  long startServiceCreate();

  void serviceCreateLatchReleased(long start);

  void serviceInitLatchReleased(long start);

  long getServiceCreateLatchTime();

  long getServiceInitLatchTime();

  // time spent waiting for grantor latches to open
  long getGrantorWaitsInProgress();

  long getGrantorWaitsCompleted();

  long getGrantorWaitsFailed();

  long getGrantorWaitTime();

  long getGrantorWaitFailedTime();

  long startGrantorWait();

  void endGrantorWait(long start, boolean success);

  // helpers for executor usage
  QueueStatHelper getSerialQueueHelper();

  PoolStatHelper getWaitingPoolHelper();

  QueueStatHelper getWaitingQueueHelper();

  // time spent by grantor threads
  long getGrantorThreadsInProgress();

  long getGrantorThreadsCompleted();

  long getGrantorThreadTime();

  long getGrantorThreadExpireAndGrantLocksTime();

  long getGrantorThreadHandleRequestTimeoutsTime();

  long getGrantorThreadRemoveUnusedTokensTime();

  long startGrantorThread();

  long endGrantorThreadExpireAndGrantLocks(long start);

  long endGrantorThreadHandleRequestTimeouts(long timing);

  void endGrantorThreadRemoveUnusedTokens(long timing);

  void endGrantorThread(long start);

  // current number of lock grantors hosted by this system member
  long getPendingRequests();

  void incPendingRequests(long val);

  // acquisition of destroyReadLock in DLockService
  long getDestroyReadWaitsInProgress();

  long getDestroyReadWaitsCompleted();

  long getDestroyReadWaitsFailed();

  long getDestroyReadWaitTime();

  long getDestroyReadWaitFailedTime();

  long startDestroyReadWait();

  void endDestroyReadWait(long start, boolean success);

  // acquisition of destroyWriteLock in DLockService
  long getDestroyWriteWaitsInProgress();

  long getDestroyWriteWaitsCompleted();

  long getDestroyWriteWaitsFailed();

  long getDestroyWriteWaitTime();

  long getDestroyWriteWaitFailedTime();

  long startDestroyWriteWait();

  void endDestroyWriteWait(long start, boolean success);

  // current number of DLockService destroy read locks held by this process
  long getDestroyReads();

  void incDestroyReads(long val);

  // current number of DLockService destroy write locks held by this process
  long getDestroyWrites();

  void incDestroyWrites(long val);

  // time for call to unlock() to complete
  long getLockReleasesInProgress();

  long getLockReleasesCompleted();

  long getLockReleaseTime();

  long startLockRelease();

  void endLockRelease(long start);

  // total number of times this member has requested to become grantor
  long getBecomeGrantorRequests();

  void incBecomeGrantorRequests();

  // current number of lock tokens used by this system member
  long getTokens();

  void incTokens(long val);

  // current number of grant tokens used by local grantors
  long getGrantTokens();

  void incGrantTokens(long val);

  // current number of lock request queues used by this system member
  long getRequestQueues();

  void incRequestQueues(long val);

  long getFreeResourcesCompleted();

  void incFreeResourcesCompleted();

  long getFreeResourcesFailed();

  void incFreeResourcesFailed();
}
