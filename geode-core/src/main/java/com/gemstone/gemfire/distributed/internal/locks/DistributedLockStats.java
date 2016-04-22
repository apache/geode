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

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.distributed.internal.*;

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
  public int getLockWaitsInProgress();
  
  /**
   * Returns the total number of waits for a distributed lock
   */
  public int getLockWaitsCompleted();
  
  public int getLockWaitsFailed();

  /**
   * Returns the total number of nanoseconds spent waiting for a distributed lock.
   */
  public long getLockWaitTime();
  
  public long getLockWaitFailedTime();

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startLockWait();

  /**
   * @param start the timestamp taken when the operation started 
   */
  public void endLockWait(long start, boolean success);

  // incSerialQueueSize everytime getWaitingQueueHelper add/remove called
  public int getWaitingQueueSize();
  public void incWaitingQueueSize(int messages);
  
  // incSerialQueueSize everytime getSerialQueueHelper add/remove called
  public int getSerialQueueSize();
  public void incSerialQueueSize(int messages);
  
  // incNumSerialThreads everytime we execute with dlock getSerialExecutor() 
  public int getNumSerialThreads();
  public void incNumSerialThreads(int threads);
  
  // incWaitingThreads for every invoke of getWaitingPoolHelper startJob/endJob
  public int getWaitingThreads();
  public void incWaitingThreads(int threads);

  // current number of lock services used by this system member 
  public int getServices();
  public void incServices(int val);

  // current number of lock grantors hosted by this system member 
  public int getGrantors();
  public void incGrantors(int val);
  
  // time spent granting of lock requests to completion
  public int getGrantWaitsInProgress();
  public int getGrantWaitsCompleted();
  public int getGrantWaitsFailed();
  public long getGrantWaitTime();
  public long getGrantWaitFailedTime();
  public long startGrantWait();
  public void endGrantWait(long start);
  public void endGrantWaitNotGrantor(long start);
  public void endGrantWaitTimeout(long start);
  public void endGrantWaitNotHolder(long start);
  public void endGrantWaitFailed(long start);
  public void endGrantWaitDestroyed(long start);
  public void endGrantWaitSuspended(long start);
  
  // time spent creating initial grantor for lock service
  public int getCreateGrantorsInProgress();
  public int getCreateGrantorsCompleted();
  public long getCreateGrantorTime();
  public long startCreateGrantor();
  public void endCreateGrantor(long start);
  
  // time spent creating each lock service
  public int getServiceCreatesInProgress();
  public int getServiceCreatesCompleted();
  public long startServiceCreate();
  public void serviceCreateLatchReleased(long start);
  public void serviceInitLatchReleased(long start);
  public long getServiceCreateLatchTime();
  public long getServiceInitLatchTime();

  // time spent waiting for grantor latches to open
  public int getGrantorWaitsInProgress();
  public int getGrantorWaitsCompleted();
  public int getGrantorWaitsFailed();
  public long getGrantorWaitTime();
  public long getGrantorWaitFailedTime();
  public long startGrantorWait();
  public void endGrantorWait(long start, boolean success);
  
  // helpers for executor usage
  public QueueStatHelper getSerialQueueHelper();
  public PoolStatHelper getWaitingPoolHelper();
  public QueueStatHelper getWaitingQueueHelper();

  // time spent by grantor threads
  public int getGrantorThreadsInProgress();
  public int getGrantorThreadsCompleted();
  public long getGrantorThreadTime();
  public long getGrantorThreadExpireAndGrantLocksTime();
  public long getGrantorThreadHandleRequestTimeoutsTime();
  public long getGrantorThreadRemoveUnusedTokensTime();
  public long startGrantorThread();
  public long endGrantorThreadExpireAndGrantLocks(long start);
  public long endGrantorThreadHandleRequestTimeouts(long timing);
  public void endGrantorThreadRemoveUnusedTokens(long timing);
  public void endGrantorThread(long start);
  
  // current number of lock grantors hosted by this system member 
  public int getPendingRequests();
  public void incPendingRequests(int val);
    
  // acquisition of destroyReadLock in DLockService
  public int getDestroyReadWaitsInProgress();
  public int getDestroyReadWaitsCompleted();
  public int getDestroyReadWaitsFailed();
  public long getDestroyReadWaitTime();
  public long getDestroyReadWaitFailedTime();
  public long startDestroyReadWait();
  public void endDestroyReadWait(long start, boolean success);

  // acquisition of destroyWriteLock in DLockService
  public int getDestroyWriteWaitsInProgress();
  public int getDestroyWriteWaitsCompleted();
  public int getDestroyWriteWaitsFailed();
  public long getDestroyWriteWaitTime();
  public long getDestroyWriteWaitFailedTime();
  public long startDestroyWriteWait();
  public void endDestroyWriteWait(long start, boolean success);
  
  // current number of DLockService destroy read locks held by this process 
  public int getDestroyReads();
  public void incDestroyReads(int val);
  
  // current number of DLockService destroy write locks held by this process 
  public int getDestroyWrites();
  public void incDestroyWrites(int val);
    
  // time for call to unlock() to complete
  public int getLockReleasesInProgress();
  public int getLockReleasesCompleted();
  public long getLockReleaseTime();
  public long startLockRelease();
  public void endLockRelease(long start);
    
  // total number of times this member has requested to become grantor
  public int getBecomeGrantorRequests();
  public void incBecomeGrantorRequests();

  // current number of lock tokens used by this system member 
  public int getTokens();
  public void incTokens(int val);

  // current number of grant tokens used by local grantors
  public int getGrantTokens();
  public void incGrantTokens(int val);

  // current number of lock request queues used by this system member 
  public int getRequestQueues();
  public void incRequestQueues(int val);

  public int getFreeResourcesCompleted();
  public void incFreeResourcesCompleted();
  public int getFreeResourcesFailed();
  public void incFreeResourcesFailed();
}

