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

import java.util.function.LongSupplier;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.QueueStatHelper;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * This class maintains statistics in GemFire about the distributed lock service.
 *
 *
 */
public class DLockStats implements DistributedLockStats {

  // -------------------------------------------------------------------------
  // Statistic "Id" Fields
  // -------------------------------------------------------------------------

  @Immutable
  private static final StatisticsType type;

  private static final int grantorsId;
  private static final int servicesId;
  private static final int tokensId;
  private static final int grantTokensId;
  private static final int requestQueuesId;
  private static final int serialQueueSizeId;
  private static final int serialThreadsId;
  private static final int waitingQueueSizeId;
  private static final int waitingThreadsId;
  private static final int lockWaitsInProgressId;
  private static final int lockWaitsCompletedId;
  private static final int lockWaitTimeId;
  private static final int lockWaitsFailedId;
  private static final int lockWaitFailedTimeId;
  private static final int grantWaitsInProgressId;
  private static final int grantWaitsCompletedId;
  private static final int grantWaitTimeId;
  private static final int grantWaitsNotGrantorId;
  private static final int grantWaitNotGrantorTimeId;
  private static final int grantWaitsTimeoutId;
  private static final int grantWaitTimeoutTimeId;
  private static final int grantWaitsNotHolderId;
  private static final int grantWaitNotHolderTimeId;
  private static final int grantWaitsFailedId;
  private static final int grantWaitFailedTimeId;
  private static final int grantWaitsSuspendedId;
  private static final int grantWaitSuspendedTimeId;
  private static final int grantWaitsDestroyedId;
  private static final int grantWaitDestroyedTimeId;
  private static final int createGrantorsInProgressId;
  private static final int createGrantorsCompletedId;
  private static final int createGrantorTimeId;
  private static final int serviceCreatesInProgressId;
  private static final int serviceCreatesCompletedId;
  private static final int serviceCreateLatchTimeId;
  private static final int serviceInitLatchTimeId;
  private static final int grantorWaitsInProgressId;
  private static final int grantorWaitsCompletedId;
  private static final int grantorWaitTimeId;
  private static final int grantorWaitsFailedId;
  private static final int grantorWaitFailedTimeId;
  private static final int grantorThreadsInProgressId;
  private static final int grantorThreadsCompletedId;
  private static final int grantorThreadExpireAndGrantLocksTimeId;
  private static final int grantorThreadHandleRequestTimeoutsTimeId;
  private static final int grantorThreadRemoveUnusedTokensTimeId;
  private static final int grantorThreadTimeId;
  private static final int pendingRequestsId;
  private static final int destroyReadWaitsInProgressId;
  private static final int destroyReadWaitsCompletedId;
  private static final int destroyReadWaitTimeId;
  private static final int destroyReadWaitsFailedId;
  private static final int destroyReadWaitFailedTimeId;
  private static final int destroyWriteWaitsInProgressId;
  private static final int destroyWriteWaitsCompletedId;
  private static final int destroyWriteWaitTimeId;
  private static final int destroyWriteWaitsFailedId;
  private static final int destroyWriteWaitFailedTimeId;
  private static final int destroyReadsId;
  private static final int destroyWritesId;
  private static final int lockReleasesInProgressId;
  private static final int lockReleasesCompletedId;
  private static final int lockReleaseTimeId;
  private static final int becomeGrantorRequestsId;
  private static final int freeResourcesCompletedId;
  private static final int freeResourcesFailedId;

  static {
    String statName = "DLockStats";
    String statDescription = "Statistics on the gemfire distribution lock service.";

    final String grantorsDesc = "The current number of lock grantors hosted by this system member.";
    final String servicesDesc = "The current number of lock services used by this system member.";
    final String tokensDesc = "The current number of lock tokens used by this system member.";
    final String grantTokensDesc = "The current number of grant tokens used by local grantors.";
    final String requestQueuesDesc =
        "The current number of lock request queues used by this system member.";
    final String serialQueueSizeDesc =
        "The number of serial distribution messages currently waiting to be processed.";
    final String serialThreadsDesc =
        "The number of threads currently processing serial/ordered messages.";
    final String waitingQueueSizeDesc =
        "The number of distribution messages currently waiting for some other resource before they can be processed.";
    final String waitingThreadsDesc =
        "The number of threads currently processing messages that had to wait for a resource.";
    final String lockWaitsInProgressDesc =
        "Current number of threads waiting for a distributed lock.";
    final String lockWaitsCompletedDesc =
        "Total number of times distributed lock wait has completed by successfully obtained the lock.";
    final String lockWaitTimeDesc =
        "Total time spent waiting for a distributed lock that was obtained.";
    final String lockWaitsFailedDesc =
        "Total number of times distributed lock wait has completed by failing to obtain the lock.";
    final String lockWaitFailedTimeDesc =
        "Total time spent waiting for a distributed lock that we failed to obtain.";
    final String grantWaitsInProgressDesc =
        "Current number of distributed lock requests being granted.";
    final String grantWaitsCompletedDesc =
        "Total number of times granting of a lock request has completed by successfully granting the lock.";
    final String grantWaitTimeDesc = "Total time spent attempting to grant a distributed lock.";
    final String grantWaitsNotGrantorDesc =
        "Total number of times granting of lock request failed because not grantor.";
    final String grantWaitNotGrantorTimeDesc =
        "Total time spent granting of lock requests that failed because not grantor.";
    final String grantWaitsTimeoutDesc =
        "Total number of times granting of lock request failed because timeout.";
    final String grantWaitTimeoutTimeDesc =
        "Total time spent granting of lock requests that failed because timeout.";
    final String grantWaitsNotHolderDesc =
        "Total number of times granting of lock request failed because reentrant was not holder.";
    final String grantWaitNotHolderTimeDesc =
        "Total time spent granting of lock requests that failed because reentrant was not holder.";
    final String grantWaitsFailedDesc =
        "Total number of times granting of lock request failed because try locks failed.";
    final String grantWaitFailedTimeDesc =
        "Total time spent granting of lock requests that failed because try locks failed.";
    final String grantWaitsSuspendedDesc =
        "Total number of times granting of lock request failed because lock service was suspended.";
    final String grantWaitSuspendedTimeDesc =
        "Total time spent granting of lock requests that failed because lock service was suspended.";
    final String grantWaitsDestroyedDesc =
        "Total number of times granting of lock request failed because lock service was destroyed.";
    final String grantWaitDestroyedTimeDesc =
        "Total time spent granting of lock requests that failed because lock service was destroyed.";
    final String createGrantorsInProgressDesc =
        "Current number of initial grantors being created in this process.";
    final String createGrantorsCompletedDesc =
        "Total number of initial grantors created in this process.";
    final String createGrantorTimeDesc =
        "Total time spent waiting create the intial grantor for lock services.";
    final String serviceCreatesInProgressDesc =
        "Current number of lock services being created in this process.";
    final String serviceCreatesCompletedDesc =
        "Total number of lock services created in this process.";
    final String serviceCreateLatchTimeDesc =
        "Total time spent creating lock services before releasing create latches.";
    final String serviceInitLatchTimeDesc =
        "Total time spent creating lock services before releasing init latches.";
    final String grantorWaitsInProgressDesc =
        "Current number of threads waiting for grantor latch to open.";
    final String grantorWaitsCompletedDesc =
        "Total number of times waiting threads completed waiting for the grantor latch to open.";
    final String grantorWaitTimeDesc =
        "Total time spent waiting for the grantor latch which resulted in success.";
    final String grantorWaitsFailedDesc =
        "Total number of times waiting threads failed to finish waiting for the grantor latch to open.";
    final String grantorWaitFailedTimeDesc =
        "Total time spent waiting for the grantor latch which resulted in failure.";
    final String grantorThreadsInProgressDesc =
        "Current iterations of work performed by grantor thread(s).";
    final String grantorThreadsCompletedDesc =
        "Total number of iterations of work performed by grantor thread(s).";
    final String grantorThreadExpireAndGrantLocksTimeDesc =
        "Total time spent by grantor thread(s) performing expireAndGrantLocks tasks.";
    final String grantorThreadHandleRequestTimeoutsTimeDesc =
        "Total time spent by grantor thread(s) performing handleRequestTimeouts tasks.";
    final String grantorThreadRemoveUnusedTokensTimeDesc =
        "Total time spent by grantor thread(s) performing removeUnusedTokens tasks.";
    final String grantorThreadTimeDesc =
        "Total time spent by grantor thread(s) performing all grantor tasks.";
    final String pendingRequestsDesc =
        "The current number of pending lock requests queued by grantors in this process.";
    final String destroyReadWaitsInProgressDesc =
        "Current number of threads waiting for a DLockService destroy read lock.";
    final String destroyReadWaitsCompletedDesc =
        "Total number of times a DLockService destroy read lock wait has completed successfully.";
    final String destroyReadWaitTimeDesc =
        "Total time spent waiting for a DLockService destroy read lock that was obtained.";
    final String destroyReadWaitsFailedDesc =
        "Total number of times a DLockService destroy read lock wait has completed unsuccessfully.";
    final String destroyReadWaitFailedTimeDesc =
        "Total time spent waiting for a DLockService destroy read lock that was not obtained.";
    final String destroyWriteWaitsInProgressDesc =
        "Current number of thwrites waiting for a DLockService destroy write lock.";
    final String destroyWriteWaitsCompletedDesc =
        "Total number of times a DLockService destroy write lock wait has completed successfully.";
    final String destroyWriteWaitTimeDesc =
        "Total time spent waiting for a DLockService destroy write lock that was obtained.";
    final String destroyWriteWaitsFailedDesc =
        "Total number of times a DLockService destroy write lock wait has completed unsuccessfully.";
    final String destroyWriteWaitFailedTimeDesc =
        "Total time spent waiting for a DLockService destroy write lock that was not obtained.";
    final String destroyReadsDesc =
        "The current number of DLockService destroy read locks held by this process.";
    final String destroyWritesDesc =
        "The current number of DLockService destroy write locks held by this process.";
    final String lockReleasesInProgressDesc =
        "Current number of threads releasing a distributed lock.";
    final String lockReleasesCompletedDesc =
        "Total number of times distributed lock release has completed.";
    final String lockReleaseTimeDesc = "Total time spent releasing a distributed lock.";
    final String becomeGrantorRequestsDesc =
        "Total number of times this member has explicitly requested to become lock grantor.";
    final String freeResourcesCompletedDesc =
        "Total number of times this member has freed resources for a distributed lock.";
    final String freeResourcesFailedDesc =
        "Total number of times this member has attempted to free resources for a distributed lock which remained in use.";

    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    type = f.createType(statName, statDescription, new StatisticDescriptor[] {
        f.createIntGauge("grantors", grantorsDesc, "grantors"),
        f.createIntGauge("services", servicesDesc, "services"),
        f.createIntGauge("tokens", tokensDesc, "tokens"),
        f.createIntGauge("grantTokens", grantTokensDesc, "grantTokens"),
        f.createIntGauge("requestQueues", requestQueuesDesc, "requestQueues"),
        f.createIntGauge("serialQueueSize", serialQueueSizeDesc, "messages"),
        f.createIntGauge("serialThreads", serialThreadsDesc, "threads"),
        f.createIntGauge("waitingQueueSize", waitingQueueSizeDesc, "messages"),
        f.createIntGauge("waitingThreads", waitingThreadsDesc, "threads"),
        f.createIntGauge("lockWaitsInProgress", lockWaitsInProgressDesc, "operations"),
        f.createIntCounter("lockWaitsCompleted", lockWaitsCompletedDesc, "operations"),
        f.createLongCounter("lockWaitTime", lockWaitTimeDesc, "nanoseconds", false),
        f.createIntCounter("lockWaitsFailed", lockWaitsFailedDesc, "operations"),
        f.createLongCounter("lockWaitFailedTime", lockWaitFailedTimeDesc, "nanoseconds", false),
        f.createIntGauge("grantWaitsInProgress", grantWaitsInProgressDesc, "operations"),
        f.createIntCounter("grantWaitsCompleted", grantWaitsCompletedDesc, "operations"),
        f.createLongCounter("grantWaitTime", grantWaitTimeDesc, "nanoseconds", false),
        f.createIntCounter("grantWaitsNotGrantor", grantWaitsNotGrantorDesc, "operations"),
        f.createLongCounter("grantWaitNotGrantorTime", grantWaitNotGrantorTimeDesc, "nanoseconds",
            false),
        f.createIntCounter("grantWaitsTimeout", grantWaitsTimeoutDesc, "operations"),
        f.createLongCounter("grantWaitTimeoutTime", grantWaitTimeoutTimeDesc, "nanoseconds", false),
        f.createIntCounter("grantWaitsNotHolder", grantWaitsNotHolderDesc, "operations"),
        f.createLongCounter("grantWaitNotHolderTime", grantWaitNotHolderTimeDesc, "nanoseconds",
            false),
        f.createIntCounter("grantWaitsFailed", grantWaitsFailedDesc, "operations"),
        f.createLongCounter("grantWaitFailedTime", grantWaitFailedTimeDesc, "nanoseconds", false),
        f.createIntCounter("grantWaitsSuspended", grantWaitsSuspendedDesc, "operations"),
        f.createLongCounter("grantWaitSuspendedTime", grantWaitSuspendedTimeDesc, "nanoseconds",
            false),
        f.createIntCounter("grantWaitsDestroyed", grantWaitsDestroyedDesc, "operations"),
        f.createLongCounter("grantWaitDestroyedTime", grantWaitDestroyedTimeDesc, "nanoseconds",
            false),
        f.createIntGauge("createGrantorsInProgress", createGrantorsInProgressDesc, "operations"),
        f.createIntCounter("createGrantorsCompleted", createGrantorsCompletedDesc, "operations"),
        f.createLongCounter("createGrantorTime", createGrantorTimeDesc, "nanoseconds", false),
        f.createIntGauge("serviceCreatesInProgress", serviceCreatesInProgressDesc, "operations"),
        f.createIntCounter("serviceCreatesCompleted", serviceCreatesCompletedDesc, "operations"),
        f.createLongCounter("serviceCreateLatchTime", serviceCreateLatchTimeDesc, "nanoseconds",
            false),
        f.createLongCounter("serviceInitLatchTime", serviceInitLatchTimeDesc, "nanoseconds", false),
        f.createIntGauge("grantorWaitsInProgress", grantorWaitsInProgressDesc, "operations"),
        f.createIntCounter("grantorWaitsCompleted", grantorWaitsCompletedDesc, "operations"),
        f.createLongCounter("grantorWaitTime", grantorWaitTimeDesc, "nanoseconds", false),
        f.createIntCounter("grantorWaitsFailed", grantorWaitsFailedDesc, "operations"),
        f.createLongCounter("grantorWaitFailedTime", grantorWaitFailedTimeDesc, "nanoseconds",
            false),
        f.createIntGauge("grantorThreadsInProgress", grantorThreadsInProgressDesc, "operations"),
        f.createIntCounter("grantorThreadsCompleted", grantorThreadsCompletedDesc, "operations"),
        f.createLongCounter("grantorThreadExpireAndGrantLocksTime",
            grantorThreadExpireAndGrantLocksTimeDesc, "nanoseconds", false),
        f.createLongCounter("grantorThreadHandleRequestTimeoutsTime",
            grantorThreadHandleRequestTimeoutsTimeDesc, "nanoseconds", false),
        f.createLongCounter("grantorThreadRemoveUnusedTokensTime",
            grantorThreadRemoveUnusedTokensTimeDesc, "nanoseconds", false),
        f.createLongCounter("grantorThreadTime", grantorThreadTimeDesc, "nanoseconds", false),
        f.createIntGauge("pendingRequests", pendingRequestsDesc, "pendingRequests"),
        f.createIntGauge("destroyReadWaitsInProgress", destroyReadWaitsInProgressDesc,
            "operations"),
        f.createIntCounter("destroyReadWaitsCompleted", destroyReadWaitsCompletedDesc,
            "operations"),
        f.createLongCounter("destroyReadWaitTime", destroyReadWaitTimeDesc, "nanoseconds", false),
        f.createIntCounter("destroyReadWaitsFailed", destroyReadWaitsFailedDesc, "operations"),
        f.createLongCounter("destroyReadWaitFailedTime", destroyReadWaitFailedTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("destroyWriteWaitsInProgress", destroyWriteWaitsInProgressDesc,
            "operations"),
        f.createIntCounter("destroyWriteWaitsCompleted", destroyWriteWaitsCompletedDesc,
            "operations"),
        f.createLongCounter("destroyWriteWaitTime", destroyWriteWaitTimeDesc, "nanoseconds", false),
        f.createIntCounter("destroyWriteWaitsFailed", destroyWriteWaitsFailedDesc, "operations"),
        f.createLongCounter("destroyWriteWaitFailedTime", destroyWriteWaitFailedTimeDesc,
            "nanoseconds", false),
        f.createIntGauge("destroyReads", destroyReadsDesc, "destroyReads"),
        f.createIntGauge("destroyWrites", destroyWritesDesc, "destroyWrites"),
        f.createIntGauge("lockReleasesInProgress", lockReleasesInProgressDesc, "operations"),
        f.createIntCounter("lockReleasesCompleted", lockReleasesCompletedDesc, "operations"),
        f.createLongCounter("lockReleaseTime", lockReleaseTimeDesc, "nanoseconds", false),
        f.createIntCounter("becomeGrantorRequests", becomeGrantorRequestsDesc, "operations"),
        f.createIntCounter("freeResourcesCompleted", freeResourcesCompletedDesc, "operations"),
        f.createIntCounter("freeResourcesFailed", freeResourcesFailedDesc, "operations"),});

    // Initialize id fields
    grantorsId = type.nameToId("grantors");
    servicesId = type.nameToId("services");
    tokensId = type.nameToId("tokens");
    grantTokensId = type.nameToId("grantTokens");
    requestQueuesId = type.nameToId("requestQueues");
    serialQueueSizeId = type.nameToId("serialQueueSize");
    serialThreadsId = type.nameToId("serialThreads");
    waitingQueueSizeId = type.nameToId("waitingQueueSize");
    waitingThreadsId = type.nameToId("waitingThreads");
    lockWaitsInProgressId = type.nameToId("lockWaitsInProgress");
    lockWaitsCompletedId = type.nameToId("lockWaitsCompleted");
    lockWaitTimeId = type.nameToId("lockWaitTime");
    lockWaitsFailedId = type.nameToId("lockWaitsFailed");
    lockWaitFailedTimeId = type.nameToId("lockWaitFailedTime");
    grantWaitsInProgressId = type.nameToId("grantWaitsInProgress");
    grantWaitsCompletedId = type.nameToId("grantWaitsCompleted");
    grantWaitTimeId = type.nameToId("grantWaitTime");
    grantWaitsNotGrantorId = type.nameToId("grantWaitsNotGrantor");
    grantWaitNotGrantorTimeId = type.nameToId("grantWaitNotGrantorTime");
    grantWaitsTimeoutId = type.nameToId("grantWaitsTimeout");
    grantWaitTimeoutTimeId = type.nameToId("grantWaitTimeoutTime");
    grantWaitsNotHolderId = type.nameToId("grantWaitsNotHolder");
    grantWaitNotHolderTimeId = type.nameToId("grantWaitNotHolderTime");
    grantWaitsFailedId = type.nameToId("grantWaitsFailed");
    grantWaitFailedTimeId = type.nameToId("grantWaitFailedTime");
    grantWaitsSuspendedId = type.nameToId("grantWaitsSuspended");
    grantWaitSuspendedTimeId = type.nameToId("grantWaitSuspendedTime");
    grantWaitsDestroyedId = type.nameToId("grantWaitsDestroyed");
    grantWaitDestroyedTimeId = type.nameToId("grantWaitDestroyedTime");
    createGrantorsInProgressId = type.nameToId("createGrantorsInProgress");
    createGrantorsCompletedId = type.nameToId("createGrantorsCompleted");
    createGrantorTimeId = type.nameToId("createGrantorTime");
    serviceCreatesInProgressId = type.nameToId("serviceCreatesInProgress");
    serviceCreatesCompletedId = type.nameToId("serviceCreatesCompleted");
    serviceCreateLatchTimeId = type.nameToId("serviceCreateLatchTime");
    serviceInitLatchTimeId = type.nameToId("serviceInitLatchTime");
    grantorWaitsInProgressId = type.nameToId("grantorWaitsInProgress");
    grantorWaitsCompletedId = type.nameToId("grantorWaitsCompleted");
    grantorWaitTimeId = type.nameToId("grantorWaitTime");
    grantorWaitsFailedId = type.nameToId("grantorWaitsFailed");
    grantorWaitFailedTimeId = type.nameToId("grantorWaitFailedTime");
    grantorThreadsInProgressId = type.nameToId("grantorThreadsInProgress");
    grantorThreadsCompletedId = type.nameToId("grantorThreadsCompleted");
    grantorThreadExpireAndGrantLocksTimeId = type.nameToId("grantorThreadExpireAndGrantLocksTime");
    grantorThreadHandleRequestTimeoutsTimeId =
        type.nameToId("grantorThreadHandleRequestTimeoutsTime");
    grantorThreadRemoveUnusedTokensTimeId = type.nameToId("grantorThreadRemoveUnusedTokensTime");
    grantorThreadTimeId = type.nameToId("grantorThreadTime");
    pendingRequestsId = type.nameToId("pendingRequests");
    destroyReadWaitsInProgressId = type.nameToId("destroyReadWaitsInProgress");
    destroyReadWaitsCompletedId = type.nameToId("destroyReadWaitsCompleted");
    destroyReadWaitTimeId = type.nameToId("destroyReadWaitTime");
    destroyReadWaitsFailedId = type.nameToId("destroyReadWaitsFailed");
    destroyReadWaitFailedTimeId = type.nameToId("destroyReadWaitFailedTime");
    destroyWriteWaitsInProgressId = type.nameToId("destroyWriteWaitsInProgress");
    destroyWriteWaitsCompletedId = type.nameToId("destroyWriteWaitsCompleted");
    destroyWriteWaitTimeId = type.nameToId("destroyWriteWaitTime");
    destroyWriteWaitsFailedId = type.nameToId("destroyWriteWaitsFailed");
    destroyWriteWaitFailedTimeId = type.nameToId("destroyWriteWaitFailedTime");
    destroyReadsId = type.nameToId("destroyReads");
    destroyWritesId = type.nameToId("destroyWrites");
    lockReleasesInProgressId = type.nameToId("lockReleasesInProgress");
    lockReleasesCompletedId = type.nameToId("lockReleasesCompleted");
    lockReleaseTimeId = type.nameToId("lockReleaseTime");
    becomeGrantorRequestsId = type.nameToId("becomeGrantorRequests");
    freeResourcesCompletedId = type.nameToId("freeResourcesCompleted");
    freeResourcesFailedId = type.nameToId("freeResourcesFailed");
  } // static block

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  private final LongSupplier clock;

  // -------------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------------

  /**
   * Creates a new <code>DLockStats</code> and registers itself with the given statistics factory.
   */
  public DLockStats(StatisticsFactory f, long statId) {
    this(f, "dlockStats", statId, DistributionStats::getStatTime);
  }

  @VisibleForTesting
  public DLockStats(StatisticsFactory factory, String textId, long statId, LongSupplier clock) {
    stats = factory == null ? null : factory.createAtomicStatistics(type, textId, statId);
    this.clock = clock;
  }


  // -------------------------------------------------------------------------
  // Instance methods
  // -------------------------------------------------------------------------

  private long getTime() {
    return clock.getAsLong();
  }

  public void close() {
    this.stats.close();
  }

  // time for call to lock() to complete
  @Override
  public int getLockWaitsInProgress() {
    return stats.getInt(lockWaitsInProgressId);
  }

  @Override
  public int getLockWaitsCompleted() {
    return stats.getInt(lockWaitsCompletedId);
  }

  @Override
  public int getLockWaitsFailed() {
    return stats.getInt(lockWaitsFailedId);
  }

  @Override
  public long getLockWaitTime() {
    return stats.getLong(lockWaitTimeId);
  }

  @Override
  public long getLockWaitFailedTime() {
    return stats.getLong(lockWaitFailedTimeId);
  }

  @Override
  public long startLockWait() {
    stats.incInt(lockWaitsInProgressId, 1);
    return getTime();
  }

  @Override
  public void endLockWait(long start, boolean success) {
    long ts = getTime();
    stats.incInt(lockWaitsInProgressId, -1);
    if (success) {
      stats.incInt(lockWaitsCompletedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(lockWaitTimeId, ts - start);
      }
    } else {
      stats.incInt(lockWaitsFailedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(lockWaitFailedTimeId, ts - start);
      }
    }
  }

  // incSerialQueueSize everytime getWaitingQueueHelper add/remove called
  @Override
  public int getWaitingQueueSize() {
    return this.stats.getInt(waitingQueueSizeId);
  }

  @Override
  public void incWaitingQueueSize(int messages) { // TODO: prolly no callers
    this.stats.incInt(waitingQueueSizeId, messages);
  }

  // incSerialQueueSize everytime getSerialQueueHelper add/remove called
  @Override
  public int getSerialQueueSize() {
    return this.stats.getInt(serialQueueSizeId);
  }

  @Override
  public void incSerialQueueSize(int messages) { // TODO: prolly no callers
    this.stats.incInt(serialQueueSizeId, messages);
  }

  // incNumSerialThreads everytime we execute with dlock getSerialExecutor()
  @Override
  public int getNumSerialThreads() {
    return this.stats.getInt(serialThreadsId);
  }

  @Override
  public void incNumSerialThreads(int threads) { // TODO: no callers!
    this.stats.incInt(serialThreadsId, threads);
  }

  // incWaitingThreads for every invoke of getWaitingPoolHelper startJob/endJob
  @Override
  public int getWaitingThreads() {
    return this.stats.getInt(waitingThreadsId);
  }

  @Override
  public void incWaitingThreads(int threads) { // TODO: prolly no callers
    this.stats.incInt(waitingThreadsId, threads);
  }

  // current number of lock services used by this system member
  @Override
  public int getServices() {
    return this.stats.getInt(servicesId);
  }

  @Override
  public void incServices(int val) {
    this.stats.incInt(servicesId, val);
  }

  // current number of lock grantors hosted by this system member
  @Override
  public int getGrantors() {
    return this.stats.getInt(grantorsId);
  }

  @Override
  public void incGrantors(int val) {
    this.stats.incInt(grantorsId, val);
  }

  // current number of lock tokens used by this system member
  @Override
  public int getTokens() {
    return this.stats.getInt(tokensId);
  }

  @Override
  public void incTokens(int val) {
    this.stats.incInt(tokensId, val);
  }

  // current number of grant tokens used by local grantors
  @Override
  public int getGrantTokens() {
    return this.stats.getInt(grantTokensId);
  }

  @Override
  public void incGrantTokens(int val) {
    this.stats.incInt(grantTokensId, val);
  }

  // current number of lock request queues used by this system member
  @Override
  public int getRequestQueues() {
    return this.stats.getInt(requestQueuesId);
  }

  @Override
  public void incRequestQueues(int val) {
    this.stats.incInt(requestQueuesId, val);
  }

  // time for granting of lock requests to complete
  @Override
  public int getGrantWaitsInProgress() {
    return stats.getInt(grantWaitsInProgressId);
  }

  @Override
  public int getGrantWaitsCompleted() {
    return stats.getInt(grantWaitsCompletedId);
  }

  @Override
  public int getGrantWaitsFailed() {
    return stats.getInt(grantWaitsFailedId);
  }

  public int getGrantWaitsSuspended() {
    return stats.getInt(grantWaitsSuspendedId);
  }

  public int getGrantWaitsDestroyed() {
    return stats.getInt(grantWaitsDestroyedId);
  }

  @Override
  public long getGrantWaitTime() {
    return stats.getLong(grantWaitTimeId);
  }

  @Override
  public long getGrantWaitFailedTime() {
    return stats.getLong(grantWaitFailedTimeId);
  }

  @Override
  public long startGrantWait() {
    stats.incInt(grantWaitsInProgressId, 1);
    return getTime();
  }

  @Override
  public void endGrantWait(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsCompletedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitTimeId, ts - start);
    }
  }

  @Override
  public void endGrantWaitNotGrantor(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsNotGrantorId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitNotGrantorTimeId, ts - start);
    }
  }

  @Override
  public void endGrantWaitTimeout(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsTimeoutId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitTimeoutTimeId, ts - start);
    }
  }

  @Override
  public void endGrantWaitNotHolder(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsNotHolderId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitNotHolderTimeId, ts - start);
    }
  }

  @Override
  public void endGrantWaitFailed(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsFailedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitFailedTimeId, ts - start);
    }
  }

  @Override
  public void endGrantWaitSuspended(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsSuspendedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitSuspendedTimeId, ts - start);
    }
  }

  @Override
  public void endGrantWaitDestroyed(long start) {
    long ts = getTime();
    stats.incInt(grantWaitsInProgressId, -1);
    stats.incInt(grantWaitsDestroyedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantWaitDestroyedTimeId, ts - start);
    }
  }

  // time for creating initial grantor for lock service
  @Override
  public int getCreateGrantorsInProgress() {
    return stats.getInt(createGrantorsInProgressId);
  }

  @Override
  public int getCreateGrantorsCompleted() {
    return stats.getInt(createGrantorsCompletedId);
  }

  @Override
  public long getCreateGrantorTime() {
    return stats.getLong(createGrantorTimeId);
  }

  @Override
  public long startCreateGrantor() { // TODO: no callers!
    stats.incInt(createGrantorsInProgressId, 1);
    return getTime();
  }

  @Override
  public void endCreateGrantor(long start) {
    long ts = getTime();
    stats.incInt(createGrantorsInProgressId, -1);
    stats.incInt(createGrantorsCompletedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(createGrantorTimeId, ts - start);
    }
  }

  // time for creating each lock service
  @Override
  public int getServiceCreatesInProgress() {
    return stats.getInt(serviceCreatesInProgressId);
  }

  @Override
  public int getServiceCreatesCompleted() {
    return stats.getInt(serviceCreatesCompletedId);
  }

  @Override
  public long startServiceCreate() { // TODO: no callers!
    stats.incInt(serviceCreatesInProgressId, 1);
    return getTime();
  }

  @Override
  public void serviceCreateLatchReleased(long start) {
    if (DistributionStats.enableClockStats) {
      long ts = getTime();
      stats.incLong(serviceCreateLatchTimeId, ts - start);
    }
  }

  @Override
  public void serviceInitLatchReleased(long start) {
    long ts = getTime();
    stats.incInt(serviceCreatesInProgressId, -1);
    stats.incInt(serviceCreatesCompletedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(serviceInitLatchTimeId, ts - start);
    }
  }

  @Override
  public long getServiceCreateLatchTime() {
    return stats.getLong(serviceCreateLatchTimeId);
  }

  @Override
  public long getServiceInitLatchTime() {
    return stats.getLong(serviceInitLatchTimeId);
  }

  // time spent waiting grantor latches
  @Override
  public int getGrantorWaitsInProgress() {
    return stats.getInt(grantorWaitsInProgressId);
  }

  @Override
  public int getGrantorWaitsCompleted() {
    return stats.getInt(grantorWaitsCompletedId);
  }

  @Override
  public int getGrantorWaitsFailed() {
    return stats.getInt(grantorWaitsFailedId);
  }

  @Override
  public long getGrantorWaitTime() {
    return stats.getLong(grantorWaitTimeId);
  }

  @Override
  public long getGrantorWaitFailedTime() {
    return stats.getLong(grantorWaitFailedTimeId);
  }

  @Override
  public long startGrantorWait() {
    stats.incInt(grantorWaitsInProgressId, 1);
    return getTime();
  }

  @Override
  public void endGrantorWait(long start, boolean success) {
    long ts = getTime();
    stats.incInt(grantorWaitsInProgressId, -1);
    if (success) {
      stats.incInt(grantorWaitsCompletedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(grantorWaitTimeId, ts - start);
      }
    } else {
      stats.incInt(grantorWaitsFailedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(grantorWaitFailedTimeId, ts - start);
      }
    }
  }

  // time spent by grantor threads
  @Override
  public int getGrantorThreadsInProgress() {
    return stats.getInt(grantorThreadsInProgressId);
  }

  @Override
  public int getGrantorThreadsCompleted() {
    return stats.getInt(grantorThreadsCompletedId);
  }

  @Override
  public long getGrantorThreadTime() {
    return stats.getLong(grantorThreadTimeId);
  }

  @Override
  public long getGrantorThreadExpireAndGrantLocksTime() {
    return stats.getLong(grantorThreadExpireAndGrantLocksTimeId);
  }

  @Override
  public long getGrantorThreadHandleRequestTimeoutsTime() {
    return stats.getLong(grantorThreadHandleRequestTimeoutsTimeId);
  }

  @Override
  public long getGrantorThreadRemoveUnusedTokensTime() {
    return stats.getLong(grantorThreadRemoveUnusedTokensTimeId);
  }

  @Override
  public long startGrantorThread() {
    stats.incInt(grantorThreadsInProgressId, 1);
    return getTime();
  }

  @Override
  public long endGrantorThreadExpireAndGrantLocks(long start) {
    long ts = getTime();
    stats.incLong(grantorThreadExpireAndGrantLocksTimeId, ts - start);
    return getTime();
  }

  @Override
  public long endGrantorThreadHandleRequestTimeouts(long timing) {
    long ts = getTime();
    stats.incLong(grantorThreadHandleRequestTimeoutsTimeId, ts - timing);
    return getTime();
  }

  @Override
  public void endGrantorThreadRemoveUnusedTokens(long timing) {
    long ts = getTime();
    stats.incLong(grantorThreadRemoveUnusedTokensTimeId, ts - timing);
  }

  @Override
  public void endGrantorThread(long start) {
    long ts = getTime();
    stats.incInt(grantorThreadsInProgressId, -1);
    stats.incInt(grantorThreadsCompletedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(grantorThreadTimeId, ts - start);
    }
  }

  // current number of requests waiting in lock grantor queues
  @Override
  public int getPendingRequests() {
    return this.stats.getInt(pendingRequestsId);
  }

  @Override
  public void incPendingRequests(int val) {
    this.stats.incInt(pendingRequestsId, val);
  }

  // acquisition of destroyReadLock in DLockService
  @Override
  public int getDestroyReadWaitsInProgress() {
    return stats.getInt(destroyReadWaitsInProgressId);
  }

  @Override
  public int getDestroyReadWaitsCompleted() {
    return stats.getInt(destroyReadWaitsCompletedId);
  }

  @Override
  public int getDestroyReadWaitsFailed() {
    return stats.getInt(destroyReadWaitsFailedId);
  }

  @Override
  public long getDestroyReadWaitTime() {
    return stats.getLong(destroyReadWaitTimeId);
  }

  @Override
  public long getDestroyReadWaitFailedTime() {
    return stats.getLong(destroyReadWaitFailedTimeId);
  }

  @Override
  public long startDestroyReadWait() { // TODO: no callers!
    stats.incInt(destroyReadWaitsInProgressId, 1);
    return getTime();
  }

  @Override
  public void endDestroyReadWait(long start, boolean success) {
    long ts = getTime();
    stats.incInt(destroyReadWaitsInProgressId, -1);
    if (success) {
      stats.incInt(destroyReadWaitsCompletedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(destroyReadWaitTimeId, ts - start);
      }
    } else {
      stats.incInt(destroyReadWaitsFailedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(destroyReadWaitFailedTimeId, ts - start);
      }
    }
  }

  // acquisition of destroyWriteLock in DLockService
  @Override
  public int getDestroyWriteWaitsInProgress() {
    return stats.getInt(destroyWriteWaitsInProgressId);
  }

  @Override
  public int getDestroyWriteWaitsCompleted() {
    return stats.getInt(destroyWriteWaitsCompletedId);
  }

  @Override
  public int getDestroyWriteWaitsFailed() {
    return stats.getInt(destroyWriteWaitsFailedId);
  }

  @Override
  public long getDestroyWriteWaitTime() {
    return stats.getLong(destroyWriteWaitTimeId);
  }

  @Override
  public long getDestroyWriteWaitFailedTime() {
    return stats.getLong(destroyWriteWaitFailedTimeId);
  }

  @Override
  public long startDestroyWriteWait() { // TODO: no callers!
    stats.incInt(destroyWriteWaitsInProgressId, 1);
    return getTime();
  }

  @Override
  public void endDestroyWriteWait(long start, boolean success) {
    long ts = getTime();
    stats.incInt(destroyWriteWaitsInProgressId, -1);
    if (success) {
      stats.incInt(destroyWriteWaitsCompletedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(destroyWriteWaitTimeId, ts - start);
      }
    } else {
      stats.incInt(destroyWriteWaitsFailedId, 1);
      if (DistributionStats.enableClockStats) {
        stats.incLong(destroyWriteWaitFailedTimeId, ts - start);
      }
    }
  }

  // current number of DLockService destroy read locks held by this process
  @Override
  public int getDestroyReads() {
    return this.stats.getInt(destroyReadsId);
  }

  @Override
  public void incDestroyReads(int val) { // TODO: no callers!
    this.stats.incInt(destroyReadsId, val);
  }

  // current number of DLockService destroy write locks held by this process
  @Override
  public int getDestroyWrites() {
    return this.stats.getInt(destroyWritesId);
  }

  @Override
  public void incDestroyWrites(int val) { // TODO: no callers!
    this.stats.incInt(destroyWritesId, val);
  }

  // time for call to unlock() to complete
  @Override
  public int getLockReleasesInProgress() {
    return stats.getInt(lockReleasesInProgressId);
  }

  @Override
  public int getLockReleasesCompleted() {
    return stats.getInt(lockReleasesCompletedId);
  }

  @Override
  public long getLockReleaseTime() {
    return stats.getLong(lockReleaseTimeId);
  }

  @Override
  public long startLockRelease() {
    stats.incInt(lockReleasesInProgressId, 1);
    return getTime();
  }

  @Override
  public void endLockRelease(long start) {
    long ts = getTime();
    stats.incInt(lockReleasesInProgressId, -1);
    stats.incInt(lockReleasesCompletedId, 1);
    if (DistributionStats.enableClockStats) {
      stats.incLong(lockReleaseTimeId, ts - start);
    }
  }

  // total number of times this member has requested to become grantor
  @Override
  public int getBecomeGrantorRequests() {
    return this.stats.getInt(becomeGrantorRequestsId);
  }

  @Override
  public void incBecomeGrantorRequests() {
    this.stats.incInt(becomeGrantorRequestsId, 1);
  }

  @Override
  public int getFreeResourcesCompleted() {
    return this.stats.getInt(freeResourcesCompletedId);
  }

  @Override
  public void incFreeResourcesCompleted() {
    this.stats.incInt(freeResourcesCompletedId, 1);
  }

  @Override
  public int getFreeResourcesFailed() {
    return this.stats.getInt(freeResourcesFailedId);
  }

  @Override
  public void incFreeResourcesFailed() {
    this.stats.incInt(freeResourcesFailedId, 1);
  }

  // -------------------------------------------------------------------------
  // StatHelpers for dedicated dlock executors
  // -------------------------------------------------------------------------

  /**
   * Returns a helper object so that the serial queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  @Override
  public QueueStatHelper getSerialQueueHelper() {
    return new QueueStatHelper() {
      @Override
      public void add() {
        incSerialQueueSize(1);
      }

      @Override
      public void remove() {
        incSerialQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incSerialQueueSize(-count);
      }
    };
  }

  /**
   * Returns a helper object so that the waiting pool can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  @Override
  public PoolStatHelper getWaitingPoolHelper() {
    return new PoolStatHelper() {
      @Override
      public void startJob() {
        incWaitingThreads(1);
      }

      @Override
      public void endJob() {
        incWaitingThreads(-1);
      }
    };
  }

  /**
   * Returns a helper object so that the waiting queue can record its stats to the proper
   * distribution stats.
   *
   * @since GemFire 3.5
   */
  @Override
  public QueueStatHelper getWaitingQueueHelper() {
    return new QueueStatHelper() {
      @Override
      public void add() {
        incWaitingQueueSize(1);
      }

      @Override
      public void remove() {
        incWaitingQueueSize(-1);
      }

      @Override
      public void remove(int count) {
        incWaitingQueueSize(-count);
      }
    };
  }

  public Statistics getStats() {
    return stats;
  }
}
