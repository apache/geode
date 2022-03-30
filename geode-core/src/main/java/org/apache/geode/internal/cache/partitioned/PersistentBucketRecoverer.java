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
package org.apache.geode.internal.cache.partitioned;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketPersistenceAdvisor;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.PRHARedundancyProvider;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.ProxyBucketRegion;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentStateListener;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.process.StartupStatus;
import org.apache.geode.internal.util.TransformUtils;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Consolidates logging during the recovery of ProxyRegionBuckets that are not hosted by this
 * member. The logger is meant to run in its own thread.
 * It uses a count down latch to determine whether the recovery is finished.
 */
public class PersistentBucketRecoverer extends RecoveryRunnable implements PersistentStateListener {

  private static final Logger logger = LogService.getLogger();

  /**
   * True when one or more buckets have reported a change in status.
   */
  private volatile boolean membershipChanged = true;

  /**
   * Sleep period between posting log entries.
   */
  private static final int SLEEP_PERIOD = 15000;

  /**
   * Used to determine when all proxy buckets have been recovered.
   */
  private final CountDownLatch allBucketsRecoveredFromDisk;

  private final List<RegionStatus> regions;

  private final StartupStatus startupStatus;

  /**
   * Creates a new PersistentBucketRecoverer.
   */
  public PersistentBucketRecoverer(PRHARedundancyProvider prhaRedundancyProvider,
      int proxyBuckets) {
    this(prhaRedundancyProvider, proxyBuckets, new StartupStatus());
  }

  private PersistentBucketRecoverer(PRHARedundancyProvider prhaRedundancyProvider, int proxyBuckets,
      StartupStatus startupStatus) {
    super(prhaRedundancyProvider);

    this.startupStatus = startupStatus;

    PartitionedRegion baseRegion =
        ColocationHelper.getLeaderRegion(redundancyProvider.getPartitionedRegion());
    List<PartitionedRegion> colocatedRegions =
        getColocatedChildRegions(baseRegion);
    List<RegionStatus> allRegions = new ArrayList<>(colocatedRegions.size() + 1);
    if (baseRegion.getDataPolicy().withPersistence()) {
      allRegions.add(new RegionStatus(baseRegion));
    }
    for (PartitionedRegion region : colocatedRegions) {
      if (region.getDataPolicy().withPersistence()) {
        allRegions.add(new RegionStatus(region));
      }
    }

    regions = Collections.unmodifiableList(allRegions);
    allBucketsRecoveredFromDisk = new CountDownLatch(proxyBuckets);
    membershipChanged = true;
    addListeners();
  }

  List<PartitionedRegion> getColocatedChildRegions(PartitionedRegion baseRegion) {
    return ColocationHelper.getColocatedChildRegions(baseRegion);
  }

  public void startLoggingThread() {
    Thread loggingThread = new LoggingThread(
        "PersistentBucketRecoverer for region "
            + redundancyProvider.getPartitionedRegion().getName(),
        false,
        this);
    loggingThread.start();
  }

  /**
   * Called when a member comes online for a bucket.
   */
  @Override
  public void memberOnline(InternalDistributedMember member, PersistentMemberID persistentID) {
    membershipChanged = true;
  }

  /**
   * Called when a member goes offline for a bucket.
   */
  @Override
  public void memberOffline(InternalDistributedMember member, PersistentMemberID persistentID) {
    membershipChanged = true;
  }

  /**
   * Called when a member is removed for a bucket.
   */
  @Override
  public void memberRemoved(PersistentMemberID persistentID, boolean revoked) {
    membershipChanged = true;
  }



  /**
   * Add this PersistentBucketRecoverer as a persistence listener to all the region's bucket
   * advisors.
   */
  private void addListeners() {
    for (RegionStatus region : regions) {
      region.addListeners();
    }
  }

  /**
   * Removes this PersistentBucketRecoverer as a persistence listener from all the region's bucket
   * advisors.
   */
  private void removeListeners() {
    for (RegionStatus region : regions) {
      region.removeListeners();
    }
  }



  /**
   * Writes a consolidated log entry every SLEEP_PERIOD that summarizes which buckets are still
   * waiting on persistent members for the region.
   */
  @Override
  public void run2() {
    try {
      boolean warningLogged = false;
      while (getLatchCount() > 0) {
        int sleepMillis = SLEEP_PERIOD;
        // reduce the first log time from 15secs so that higher layers can
        // report sooner to user
        if (!warningLogged) {
          sleepMillis = SLEEP_PERIOD / 2;
        }
        Thread.sleep(sleepMillis);

        if (membershipChanged) {
          membershipChanged = false;
          for (RegionStatus region : regions) {
            region.logWaitingForMembers();
          }
          warningLogged = true;
        }
      }

    } catch (InterruptedException e) {
      // Log and bail
      logger.error(e.getMessage(), e);
    } finally {
      /*
       * Our job is done. Stop listening to the bucket advisors.
       */
      removeListeners();
      /*
       * Make sure the recovery completion message was printed to the log.
       */
      for (RegionStatus region : regions) {
        if (!region.loggedDoneMessage) {
          region.logDoneMessage();
        }
      }
    }
  }


  /**
   * Keeps track of logging a message for a single partitioned region and logging a separate message
   * when the waiting is done for the same region
   *
   */
  private class RegionStatus {
    /**
     * The persistent identifier of the member running this PersistentBucketRecoverer.
     */
    private final PersistentMemberID thisMember;

    /**
     * The region that the proxy buckets belong to.
     */
    private final String region;

    /**
     * An array of ProxyBucketRegions that comprise this partitioned region.
     */
    private final ProxyBucketRegion[] bucketRegions;

    /**
     * Indicates that a completion message has been logged.
     */
    private volatile boolean loggedDoneMessage = true;

    public RegionStatus(PartitionedRegion region) {
      thisMember = createPersistentMemberID(region);
      this.region = region.getFullPath();
      bucketRegions = region.getRegionAdvisor().getProxyBucketArray();
    }

    public void removeListeners() {
      for (ProxyBucketRegion proxyBucket : bucketRegions) {
        proxyBucket.getPersistenceAdvisor().removeListener(PersistentBucketRecoverer.this);
      }
    }

    public void addListeners() {
      for (ProxyBucketRegion proxyBucket : bucketRegions) {
        proxyBucket.getPersistenceAdvisor().addListener(PersistentBucketRecoverer.this);
      }
    }

    /**
     * Creates a temporary (and somewhat fake) PersistentMemberID for this member if there is no
     * DiskStore available for our region (which can happen in some colocated scenarios).
     */
    private PersistentMemberID createPersistentMemberID(PartitionedRegion region) {
      DiskStoreImpl diskStore = null;

      /*
       * A non-persistent colocated region will not have a disk store so check the leader region if
       * this region does not have one.
       */
      if (region.getAttributes().getDataPolicy().withPersistence()) {
        diskStore = region.getDiskStore();
      } else if (ColocationHelper.getLeaderRegion(region).getAttributes().getDataPolicy()
          .withPersistence()) {
        diskStore = ColocationHelper.getLeaderRegion(region).getDiskStore();
      }

      /*
       * We have a DiskStore? Great! Simply have it generate the id.
       */
      if (null != diskStore) {
        return diskStore.generatePersistentID();
      }

      /*
       * Bummer. No DiskStore. Put together a fake one (for logging only).
       */
      {
        String name = "No name for this member";
        String diskDir = System.getProperty("user.dir");
        InetAddress localHost = null;

        try {
          localHost = LocalHostUtil.getLocalHost();
        } catch (UnknownHostException e) {
          logger.error("Could not determine my own host", e);
        }

        return (new PersistentMemberID(null, localHost, diskDir, name,
            redundancyProvider.getPartitionedRegion().getCache().cacheTimeMillis(), (short) 0));
      }
    }

    /**
     * Returns a unique Set of persistent members that all the ProxyBucketRegions are waiting for.
     *
     * @param offlineOnly true if only the members which are not currently try running should be
     *        returned, false to return all members that this member is waiting for, including
     *        members which are running but not fully initialized.
     */
    private Map<PersistentMemberID, Set<Integer>> getMembersToWaitFor(boolean offlineOnly) {
      Map<PersistentMemberID, Set<Integer>> waitingForMembers =
          new HashMap<PersistentMemberID, Set<Integer>>();


      for (ProxyBucketRegion proxyBucket : bucketRegions) {
        Integer bucketId = proxyBucket.getBucketId();

        // Get the set of missing members from the persistence advisor
        Set<PersistentMemberID> missingMembers;
        BucketPersistenceAdvisor persistenceAdvisor = proxyBucket.getPersistenceAdvisor();
        if (offlineOnly) {
          missingMembers = persistenceAdvisor.getMissingMembers();
        } else {
          missingMembers = persistenceAdvisor.getAllMembersToWaitFor();
        }

        if (missingMembers != null) {
          for (PersistentMemberID missingMember : missingMembers) {
            Set<Integer> buckets = waitingForMembers.get(missingMember);
            if (buckets == null) {
              buckets = new TreeSet<Integer>();
              waitingForMembers.put(missingMember, buckets);
            }
            buckets.add(bucketId);
          }
        }
      }

      return waitingForMembers;
    }

    /**
     * Prints a recovery completion message to the log.
     */
    private void logDoneMessage() {
      loggedDoneMessage = true;
      startupStatus.startup(
          String.format(
              "Region %s has successfully completed waiting for other members to recover the latest data. My persistent member information:%s",
              region,
              TransformUtils.persistentMemberIdToLogEntryTransformer.transform(thisMember)));
    }

    /**
     * Logs a consolidated log entry for all ProxyBucketRegions waiting for persistent members.
     */
    private void logWaitingForMembers() {
      Map<PersistentMemberID, Set<Integer>> offlineMembers = getMembersToWaitFor(true);
      Map<PersistentMemberID, Set<Integer>> allMembersToWaitFor = getMembersToWaitFor(false);

      boolean thereAreBucketsToBeRecovered = (getLatchCount() > 0);

      /*
       * Log any offline members the region is waiting for.
       */
      if (thereAreBucketsToBeRecovered && !offlineMembers.isEmpty()) {
        Set<String> membersToWaitForLogEntries = new HashSet<>();

        TransformUtils.transform(offlineMembers.entrySet(), membersToWaitForLogEntries,
            TransformUtils.persistentMemberEntryToLogEntryTransformer);

        Set<Integer> missingBuckets = getAllWaitingBuckets(offlineMembers);

        startupStatus.startup(
            String.format(
                "Region %s (and any colocated sub-regions) has potentially stale data.  Buckets %s are waiting for another offline member to recover the latest data. My persistent id is:%sOffline members with potentially new data:%sUse the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.",
                region, missingBuckets,
                TransformUtils.persistentMemberIdToLogEntryTransformer.transform(thisMember),
                membersToWaitForLogEntries));

        loggedDoneMessage = false;
      }
      /*
       * No offline? Then log any online members the region is waiting for.
       */
      else if (thereAreBucketsToBeRecovered && !allMembersToWaitFor.isEmpty()) {
        Set<String> membersToWaitForLogEntries = new HashSet<>();

        Set<Integer> missingBuckets = getAllWaitingBuckets(allMembersToWaitFor);
        TransformUtils.transform(allMembersToWaitFor.entrySet(), membersToWaitForLogEntries,
            TransformUtils.persistentMemberEntryToLogEntryTransformer);

        startupStatus.startup(
            String.format(
                "Region %s (and any colocated sub-regions) has potentially stale data.  Buckets %s are waiting for another online member to recover the latest data. My persistent id is:%sOnline members with potentially new data:%sUse the gfsh show missing-disk-stores command to see all disk stores that are being waited on by other members.",
                region, missingBuckets,
                TransformUtils.persistentMemberIdToLogEntryTransformer.transform(thisMember),
                membersToWaitForLogEntries));


        loggedDoneMessage = false;
      }
      /*
       * No online? Then log that we are done.
       */
      else if (!this.loggedDoneMessage) {
        logDoneMessage();
      }
    }

    /**
     * Get a consolodated set of all buckets that are waiting.
     */
    private Set<Integer> getAllWaitingBuckets(
        Map<PersistentMemberID, Set<Integer>> offlineMembers) {
      Set<Integer> allWaitingBuckets = new TreeSet<Integer>();
      for (Set<Integer> missingPerMember : offlineMembers.values()) {
        allWaitingBuckets.addAll(missingPerMember);
      }
      return allWaitingBuckets;
    }
  }

  public void await(long timeout, TimeUnit unit) {
    boolean interrupted = false;
    while (true) {
      try {
        redundancyProvider.getPartitionedRegion().getCancelCriterion().checkCancelInProgress(null);
        boolean done = allBucketsRecoveredFromDisk.await(timeout, unit);
        if (done) {
          break;
        }
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public void await() {
    boolean interrupted = false;
    while (true) {
      try {
        getAllBucketsRecoveredFromDiskLatch().await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  public void countDown() {
    allBucketsRecoveredFromDisk.countDown();
  }

  public void countDown(int size) {
    while (size > 0) {
      allBucketsRecoveredFromDisk.countDown();
      --size;
    }
  }

  public boolean hasRecoveryCompleted() {
    if (getLatchCount() > 0) {
      return false;
    }
    return true;
  }

  long getLatchCount() {
    return allBucketsRecoveredFromDisk.getCount();
  }

  CountDownLatch getAllBucketsRecoveredFromDiskLatch() {
    return allBucketsRecoveredFromDisk;
  }

}
