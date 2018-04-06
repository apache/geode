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
  int getLockWaitsInProgress();

  /**
   * Returns the total number of waits for a distributed lock
   */
  int getLockWaitsCompleted();

  int getLockWaitsFailed();

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
  int getWaitingQueueSize();

  void incWaitingQueueSize(int messages);

  // incSerialQueueSize everytime getSerialQueueHelper add/remove called
  int getSerialQueueSize();

  void incSerialQueueSize(int messages);

  // incNumSerialThreads everytime we execute with dlock getSerialExecutor()
  int getNumSerialThreads();

  void incNumSerialThreads(int threads);

  // incWaitingThreads for every invoke of getWaitingPoolHelper startJob/endJob
  int getWaitingThreads();

  void incWaitingThreads(int threads);

  // current number of lock services used by this system member
  int getServices();

  void incServices(int val);

  // current number of lock grantors hosted by this system member
  int getGrantors();

  void incGrantors(int val);

  // time spent granting of lock requests to completion
  int getGrantWaitsInProgress();

  int getGrantWaitsCompleted();

  int getGrantWaitsFailed();

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
  int getCreateGrantorsInProgress();

  int getCreateGrantorsCompleted();

  long getCreateGrantorTime();

  long startCreateGrantor();

  void endCreateGrantor(long start);

  // time spent creating each lock service
  int getServiceCreatesInProgress();

  int getServiceCreatesCompleted();

  long startServiceCreate();

  void serviceCreateLatchReleased(long start);

  void serviceInitLatchReleased(long start);

  long getServiceCreateLatchTime();

  long getServiceInitLatchTime();

  // time spent waiting for grantor latches to open
  int getGrantorWaitsInProgress();

  int getGrantorWaitsCompleted();

  int getGrantorWaitsFailed();

  long getGrantorWaitTime();

  long getGrantorWaitFailedTime();

  long startGrantorWait();

  void endGrantorWait(long start, boolean success);

  // helpers for executor usage
  QueueStatHelper getSerialQueueHelper();

  PoolStatHelper getWaitingPoolHelper();

  QueueStatHelper getWaitingQueueHelper();

  // time spent by grantor threads
  int getGrantorThreadsInProgress();

  int getGrantorThreadsCompleted();

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
  int getPendingRequests();

  void incPendingRequests(int val);

  // acquisition of destroyReadLock in DLockService
  int getDestroyReadWaitsInProgress();

  int getDestroyReadWaitsCompleted();

  int getDestroyReadWaitsFailed();

  long getDestroyReadWaitTime();

  long getDestroyReadWaitFailedTime();

  long startDestroyReadWait();

  void endDestroyReadWait(long start, boolean success);

  // acquisition of destroyWriteLock in DLockService
  int getDestroyWriteWaitsInProgress();

  int getDestroyWriteWaitsCompleted();

  int getDestroyWriteWaitsFailed();

  long getDestroyWriteWaitTime();

  long getDestroyWriteWaitFailedTime();

  long startDestroyWriteWait();

  void endDestroyWriteWait(long start, boolean success);

  // current number of DLockService destroy read locks held by this process
  int getDestroyReads();

  void incDestroyReads(int val);

  // current number of DLockService destroy write locks held by this process
  int getDestroyWrites();

  void incDestroyWrites(int val);

  // time for call to unlock() to complete
  int getLockReleasesInProgress();

  int getLockReleasesCompleted();

  long getLockReleaseTime();

  long startLockRelease();

  void endLockRelease(long start);

  // total number of times this member has requested to become grantor
  int getBecomeGrantorRequests();

  void incBecomeGrantorRequests();

  // current number of lock tokens used by this system member
  int getTokens();

  void incTokens(int val);

  // current number of grant tokens used by local grantors
  int getGrantTokens();

  void incGrantTokens(int val);

  // current number of lock request queues used by this system member
  int getRequestQueues();

  void incRequestQueues(int val);

  int getFreeResourcesCompleted();

  void incFreeResourcesCompleted();

  int getFreeResourcesFailed();

  void incFreeResourcesFailed();
}
