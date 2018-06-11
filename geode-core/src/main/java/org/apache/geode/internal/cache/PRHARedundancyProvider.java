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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.OneTaskOnlyExecutor;
import org.apache.geode.internal.cache.PartitionedRegion.RetryTimeKeeper;
import org.apache.geode.internal.cache.PartitionedRegionDataStore.CreateBucketResult;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.cache.partitioned.BucketBackupMessage;
import org.apache.geode.internal.cache.partitioned.CreateBucketMessage;
import org.apache.geode.internal.cache.partitioned.CreateMissingBucketsTask;
import org.apache.geode.internal.cache.partitioned.EndBucketCreationMessage;
import org.apache.geode.internal.cache.partitioned.FetchPartitionDetailsMessage;
import org.apache.geode.internal.cache.partitioned.FetchPartitionDetailsMessage.FetchPartitionDetailsResponse;
import org.apache.geode.internal.cache.partitioned.InternalPRInfo;
import org.apache.geode.internal.cache.partitioned.InternalPartitionDetails;
import org.apache.geode.internal.cache.partitioned.LoadProbe;
import org.apache.geode.internal.cache.partitioned.ManageBackupBucketMessage;
import org.apache.geode.internal.cache.partitioned.ManageBucketMessage;
import org.apache.geode.internal.cache.partitioned.ManageBucketMessage.NodeResponse;
import org.apache.geode.internal.cache.partitioned.OfflineMemberDetails;
import org.apache.geode.internal.cache.partitioned.OfflineMemberDetailsImpl;
import org.apache.geode.internal.cache.partitioned.PRLoad;
import org.apache.geode.internal.cache.partitioned.PartitionMemberInfoImpl;
import org.apache.geode.internal.cache.partitioned.PartitionRegionInfoImpl;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOp;
import org.apache.geode.internal.cache.partitioned.RecoveryRunnable;
import org.apache.geode.internal.cache.partitioned.RedundancyLogger;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import org.apache.geode.internal.cache.partitioned.rebalance.CompositeDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.FPRDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.RebalanceDirector;
import org.apache.geode.internal.cache.persistence.MembershipFlushRequest;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentStateListener;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * This class provides the redundancy management for partitioned region. It will provide the
 * following to the PartitionedRegion: <br>
 * (1) Redundancy Management at the time of bucket creation.</br>
 * <br>
 * (2) Redundancy management at the new node arrival.</br>
 * <br>
 * (3) Redundancy management when the node leaves the partitioned region distributed system
 * gracefully. i.e. Cache.close()</br>
 * <br>
 * (4) Redundancy management at random node failure.</br>
 *
 */
public class PRHARedundancyProvider {
  private static final Logger logger = LogService.getLogger();

  private static final boolean DISABLE_CREATE_BUCKET_RANDOMNESS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_CREATE_BUCKET_RANDOMNESS");

  public static class ArrayListWithClearState<T> extends ArrayList<T> {
    private static final long serialVersionUID = 1L;
    private boolean wasCleared = false;

    public boolean wasCleared() {
      return this.wasCleared;
    }

    @Override
    public void clear() {
      super.clear();
      this.wasCleared = true;
    }
  }

  public static final String DATASTORE_DISCOVERY_TIMEOUT_PROPERTY_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "partitionedRegionDatastoreDiscoveryTimeout";
  static volatile Long DATASTORE_DISCOVERY_TIMEOUT_MILLISECONDS =
      Long.getLong(DATASTORE_DISCOVERY_TIMEOUT_PROPERTY_NAME);

  public final PartitionedRegion prRegion;
  private static AtomicLong insufficientLogTimeStamp = new AtomicLong(0);
  private final AtomicBoolean firstInsufficentStoresLogged = new AtomicBoolean(false);

  /**
   * An executor to submit tasks for redundancy recovery too. It makes sure that there will only be
   * one redundancy recovery task in the queue at a time.
   */
  protected final OneTaskOnlyExecutor recoveryExecutor;
  private volatile ScheduledFuture<?> recoveryFuture;
  private final Object shutdownLock = new Object();
  private boolean shutdown = false;

  volatile CountDownLatch allBucketsRecoveredFromDisk;

  /**
   * Used to consolidate logging for bucket regions waiting on other members to come online.
   */
  private RedundancyLogger redundancyLogger = null;

  /**
   * Constructor for PRHARedundancyProvider.
   *
   * @param region The PartitionedRegion for which the HA redundancy is required to be managed.
   */
  public PRHARedundancyProvider(final PartitionedRegion region) {
    this.prRegion = region;
    final InternalResourceManager resourceManager =
        region.getGemFireCache().getInternalResourceManager();
    recoveryExecutor = new OneTaskOnlyExecutor(resourceManager.getExecutor(),
        new OneTaskOnlyExecutor.ConflatedTaskListener() {
          public void taskDropped() {
            InternalResourceManager.getResourceObserver().recoveryConflated(region);
          }
        });
  }

  /**
   * Display bucket allocation status
   *
   * @param prRegion the given region
   * @param allStores the list of available stores. If null, unknown.
   * @param alreadyUsed stores allocated; only used if allStores != null
   * @param forLog true if the generated string is for a log message
   * @return the description string
   */
  public static String regionStatus(PartitionedRegion prRegion, Set allStores,
      Collection alreadyUsed, boolean forLog) {
    StringBuffer sb = new StringBuffer();
    sb.append("Partitioned Region name = " + prRegion.getFullPath());
    final char newLine;
    final String spaces;
    if (forLog) {
      newLine = ' ';
      spaces = "";
    } else {
      newLine = '\n';
      spaces = "   ";
    }
    if (allStores != null) {
      sb.append(newLine + spaces + "Redundancy level set to " + prRegion.getRedundantCopies());
      sb.append(newLine + ". Number of available data stores: " + allStores.size());
      sb.append(newLine + spaces + ". Number successfully allocated = " + alreadyUsed.size());
      sb.append(newLine + ". Data stores: " + PartitionedRegionHelper.printCollection(allStores));
      sb.append(newLine + ". Data stores successfully allocated: "
          + PartitionedRegionHelper.printCollection(alreadyUsed));
      sb.append(newLine + ". Equivalent members: " + PartitionedRegionHelper
          .printCollection(prRegion.getDistributionManager().getMembersInThisZone()));
    }
    return sb.toString();
  }

  public static final StringId TIMEOUT_MSG =
      LocalizedStrings.PRHARedundancyProvider_IF_YOUR_SYSTEM_HAS_SUFFICIENT_SPACE_PERHAPS_IT_IS_UNDER_MEMBERSHIP_OR_REGION_CREATION_STRESS;

  /**
   * Indicate a timeout due to excessive retries among available peers
   *
   * @param allStores all feasible stores. If null, we don't know.
   * @param alreadyUsed those that have already accepted, only used if allStores != null
   * @param opString description of the operation which timed out
   */
  public static void timedOut(PartitionedRegion prRegion, Set allStores, Collection alreadyUsed,
      String opString, long timeOut) {
    final String tooManyRetries =
        LocalizedStrings.PRHARedundancyProvider_TIMED_OUT_ATTEMPTING_TO_0_IN_THE_PARTITIONED_REGION__1_WAITED_FOR_2_MS
            .toLocalizedString(new Object[] {opString,
                regionStatus(prRegion, allStores, alreadyUsed, true), Long.valueOf(timeOut)})
            + TIMEOUT_MSG;
    throw new PartitionedRegionStorageException(tooManyRetries);
  }

  private Set<InternalDistributedMember> getAllStores(String partitionName) {
    if (partitionName != null) {

      return getFixedPartitionStores(partitionName);
    }
    final Set<InternalDistributedMember> allStores =
        this.prRegion.getRegionAdvisor().adviseDataStore(true);
    PartitionedRegionDataStore myDS = this.prRegion.getDataStore();
    if (myDS != null) {
      allStores.add(this.prRegion.getDistributionManager().getId());
    }
    return allStores;
  }

  /**
   * This is for FPR, for given partition, we have to return the set of datastores on which the
   * given partition is defined
   *
   * @param partitionName name of the partition for which datastores need to be found out
   */
  private Set<InternalDistributedMember> getFixedPartitionStores(String partitionName) {
    Set<InternalDistributedMember> members =
        this.prRegion.getRegionAdvisor().adviseFixedPartitionDataStores(partitionName);

    List<FixedPartitionAttributesImpl> FPAs = this.prRegion.getFixedPartitionAttributesImpl();

    if (FPAs != null) {
      for (FixedPartitionAttributesImpl fpa : FPAs) {
        if (fpa.getPartitionName().equals(partitionName)) {
          members.add((InternalDistributedMember) this.prRegion.getMyId());
        }
      }
    }
    return members;
  }

  /**
   * Signature string indicating that not enough stores are available.
   */
  public static final StringId INSUFFICIENT_STORES_MSG =
      LocalizedStrings.PRHARedundancyProvider_CONSIDER_STARTING_ANOTHER_MEMBER;

  /**
   * Signature string indicating that there are enough stores available.
   */
  public static final StringId SUFFICIENT_STORES_MSG =
      LocalizedStrings.PRHARRedundancyProvider_FOUND_A_MEMBER_TO_HOST_A_BUCKET;

  /**
   * string indicating the attempt to allocate a bucket
   */
  private static final StringId ALLOCATE_ENOUGH_MEMBERS_TO_HOST_BUCKET =
      LocalizedStrings.PRHARRedundancyProvider_ALLOCATE_ENOUGH_MEMBERS_TO_HOST_BUCKET;


  /**
   * Indicate that we are unable to allocate sufficient stores and the timeout period has passed
   *
   * @param allStores stores we know about
   * @param alreadyUsed ones already committed
   * @param onlyLog true if only a warning log messages should be generated.
   */
  private void insufficientStores(Set allStores, Collection alreadyUsed, boolean onlyLog) {
    final String regionStat = regionStatus(this.prRegion, allStores, alreadyUsed, onlyLog);
    final char newLine;
    if (onlyLog) {
      newLine = ' ';
    } else {
      newLine = '\n';
    }
    final StringId notEnoughValidNodes;
    if (alreadyUsed.isEmpty()) {
      notEnoughValidNodes =
          LocalizedStrings.PRHARRedundancyProvider_UNABLE_TO_FIND_ANY_MEMBERS_TO_HOST_A_BUCKET_IN_THE_PARTITIONED_REGION_0;
    } else {
      notEnoughValidNodes =
          LocalizedStrings.PRHARRedundancyProvider_CONFIGURED_REDUNDANCY_LEVEL_COULD_NOT_BE_SATISFIED_0;
    }
    final Object[] notEnoughValidNodesArgs = new Object[] {
        PRHARedundancyProvider.INSUFFICIENT_STORES_MSG, newLine + regionStat + newLine};
    if (onlyLog) {
      logger.warn(LocalizedMessage.create(notEnoughValidNodes, notEnoughValidNodesArgs));
    } else {
      throw new PartitionedRegionStorageException(
          notEnoughValidNodes.toLocalizedString(notEnoughValidNodesArgs));
    }
  }

  /**
   * Create a single copy of this bucket on one node. The bucket must already be locked.
   *
   * @param bucketId The bucket we are working on
   * @param newBucketSize size to create it
   * @param alreadyUsed members who already seem to have the bucket
   * @param timeOut point at which to fail
   * @param allStores the set of data stores to choose from
   * @return the new member, null if it fails.
   * @throws PartitionedRegionStorageException if there are not enough data stores
   */
  private InternalDistributedMember createBucketInstance(int bucketId, final int newBucketSize,
      final Set<InternalDistributedMember> excludedMembers,
      Collection<InternalDistributedMember> alreadyUsed,
      ArrayListWithClearState<InternalDistributedMember> failedMembers, final long timeOut,
      final Set<InternalDistributedMember> allStores) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    // Recalculate list of candidates
    HashSet<InternalDistributedMember> candidateMembers =
        new HashSet<InternalDistributedMember>(allStores);
    candidateMembers.removeAll(alreadyUsed);
    candidateMembers.removeAll(excludedMembers);
    candidateMembers.removeAll(failedMembers);

    if (isDebugEnabled) {
      logger.debug("AllStores={} AlreadyUsed={} excluded={} failed={}", allStores, alreadyUsed,
          excludedMembers, failedMembers);
    }
    if (candidateMembers.size() == 0) {
      this.prRegion.checkReadiness(); // fix for bug #37207

      // Run out of candidates. Refetch?
      if (System.currentTimeMillis() > timeOut) {
        if (isDebugEnabled) {
          logger.debug("createBucketInstance: ran out of candidates and timed out");
        }
        return null; // fail, let caller signal error
      }

      // Recalculate
      candidateMembers = new HashSet<InternalDistributedMember>(allStores);
      candidateMembers.removeAll(alreadyUsed);
      candidateMembers.removeAll(excludedMembers);
      failedMembers.clear();
    }

    if (isDebugEnabled) {
      logger.debug("createBucketInstance: candidateMembers = {}", candidateMembers);
    }
    InternalDistributedMember candidate = null;

    // If there are no candidates, early out.
    if (candidateMembers.size() == 0) { // no options
      if (isDebugEnabled) {
        logger.debug("createBucketInstance: no valid candidates");
      }
      return null; // failure
    } // no options
    else {
      // In case of FPR, candidateMembers is the set of members on which
      // required fixed partition is defined.
      if (this.prRegion.isFixedPartitionedRegion()) {
        candidate = candidateMembers.iterator().next();
      } else {
        String prName = this.prRegion.getAttributes().getPartitionAttributes().getColocatedWith();
        if (prName != null) {
          candidate = getColocatedDataStore(candidateMembers, alreadyUsed, bucketId, prName);
        } else {
          final ArrayList<InternalDistributedMember> orderedCandidates =
              new ArrayList<InternalDistributedMember>(candidateMembers);
          candidate = getPreferredDataStore(orderedCandidates, alreadyUsed);
        }
      }
    }

    if (candidate == null) {
      failedMembers.addAll(candidateMembers);
      return null;
    }

    if (!this.prRegion.isShadowPR()
        && !ColocationHelper.checkMembersColocation(this.prRegion, candidate)) {
      if (isDebugEnabled) {
        logger.debug(
            "createBucketInstances - Member does not have all of the regions colocated with prRegion {}",
            candidate);
      }
      failedMembers.add(candidate);
      return null;
    }

    if (!(candidate.equals(this.prRegion.getMyId()))) { // myself
      PartitionProfile pp = this.prRegion.getRegionAdvisor().getPartitionProfile(candidate);
      if (pp == null) {
        if (isDebugEnabled) {
          logger.debug("createBucketInstance: {}: no partition profile for {}",
              this.prRegion.getFullPath(), candidate);
        }
        failedMembers.add(candidate);
        return null;
      }
    } // myself

    // Coordinate with any remote close occurring, causing it to wait until
    // this create bucket attempt has been made.
    final ManageBucketRsp response =
        createBucketOnMember(bucketId, candidate, newBucketSize, failedMembers.wasCleared());

    // Add targetNode to bucketNodes if successful, else to failedNodeList
    if (response.isAcceptance()) {
      return candidate; // success!
    }

    if (isDebugEnabled) {
      logger.debug("createBucketInstance: {}: candidate {} declined to manage bucketId={}: {}",
          this.prRegion.getFullPath(), candidate, this.prRegion.bucketStringForLogs(bucketId),
          response);
    }
    if (response.equals(ManageBucketRsp.CLOSED)) {
      excludedMembers.add(candidate);
    } else {
      failedMembers.add(candidate);
    }
    candidate = null; // failure

    return null;
  }

  public static final long INSUFFICIENT_LOGGING_THROTTLE_TIME = TimeUnit.SECONDS.toNanos(
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "InsufficientLoggingThrottleTime", 2)
          .intValue());
  public static volatile boolean TEST_MODE = false;
  // since 6.6, please use the distributed system property enforce-unique-host instead.
  // public static final boolean ENFORCE_UNIQUE_HOST_STORAGE_ALLOCATION =
  // DistributionConfig.DEFAULT_ENFORCE_UNIQUE_HOST;

  public InternalDistributedMember createBucketOnDataStore(int bucketId, int size, long startTime,
      RetryTimeKeeper snoozer) {
    Set<InternalDistributedMember> attempted = new HashSet<InternalDistributedMember>();
    InternalDistributedMember ret;
    InternalDistributedMember primaryForFixedPartition = null;
    if (this.prRegion.isFixedPartitionedRegion()) {
      primaryForFixedPartition =
          this.prRegion.getRegionAdvisor().adviseFixedPrimaryPartitionDataStore(bucketId);
    }
    final boolean isDebugEnabled = logger.isDebugEnabled();

    do {
      this.prRegion.checkReadiness();
      Set<InternalDistributedMember> available =
          this.prRegion.getRegionAdvisor().adviseInitializedDataStore();
      InternalDistributedMember target = null;
      available.removeAll(attempted);
      for (InternalDistributedMember member : available) {
        if (available.contains(primaryForFixedPartition)) {
          target = primaryForFixedPartition;
        } else {
          target = member;
        }
        break;
      }
      if (target == null) {
        if (shouldLogInsufficientStores()) {
          insufficientStores(available, Collections.emptySet(), true);
        }
        // this will always throw an exception
        insufficientStores(available, Collections.emptySet(), false);
      }
      try {
        if (isDebugEnabled) {
          logger.debug("Attempting to get data store {} to create the bucket {} for us", target,
              this.prRegion.bucketStringForLogs(bucketId));
        }
        CreateBucketMessage.NodeResponse response =
            CreateBucketMessage.send(target, this.prRegion, bucketId, size);
        ret = response.waitForResponse();
        if (ret != null) {
          return ret;
        }
      } catch (ForceReattemptException e) {
        // do nothing, we will already check again for a primary.
      }
      attempted.add(target);
    } while ((ret = this.prRegion.getNodeForBucketWrite(bucketId, snoozer)) == null);
    return ret;
  }

  /**
   * Creates bucket atomically by creating all the copies to satisfy redundancy. In case all copies
   * can not be created, a PartitionedRegionStorageException is thrown to the user and
   * BucketBackupMessage is sent to the nodes to make copies of a bucket that was only partially
   * created. Other VMs are informed of bucket creation through updates through their
   * {@link BucketAdvisor.BucketProfile}s.
   *
   * <p>
   * This method is synchronized to enforce a single threaded ordering, allowing for a more accurate
   * picture of bucket distribution in the face of concurrency. See bug 37275.
   * </p>
   *
   * This method is now slightly misnamed. Another member could be in the process of creating this
   * same bucket at the same time.
   *
   * @param bucketId Id of the bucket to be created.
   * @param newBucketSize size of the first entry.
   * @param startTime a time stamp prior to calling the method, used to update bucket creation stats
   * @return the primary member for the newly created bucket
   * @throws PartitionedRegionStorageException if required # of buckets can not be created to
   *         satisfy redundancy.
   * @throws PartitionedRegionException if d-lock can not be acquired to create bucket.
   * @throws PartitionOfflineException if persistent data recovery is not complete for a partitioned
   *         region referred to in the query.
   */
  public InternalDistributedMember createBucketAtomically(final int bucketId,
      final int newBucketSize, final long startTime, final boolean finishIncompleteCreation,
      String partitionName) throws PartitionedRegionStorageException, PartitionedRegionException,
      PartitionOfflineException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    prRegion.checkPROffline();

    // If there are insufficient stores throw *before* we try acquiring the
    // (very expensive) bucket lock or the (somewhat expensive) monitor on this
    earlySufficientStoresCheck(partitionName);

    synchronized (this) {
      if (this.prRegion.getCache().isCacheAtShutdownAll()) {
        throw prRegion.getCache().getCacheClosedException("Cache is shutting down");
      }

      if (isDebugEnabled) {
        logger.debug("Starting atomic creation of bucketId={}",
            this.prRegion.bucketStringForLogs(bucketId));
      }
      Collection<InternalDistributedMember> acceptedMembers =
          new ArrayList<InternalDistributedMember>(); // ArrayList<DataBucketStores>
      Set<InternalDistributedMember> excludedMembers = new HashSet<InternalDistributedMember>();
      ArrayListWithClearState<InternalDistributedMember> failedMembers =
          new ArrayListWithClearState<InternalDistributedMember>();
      final long timeOut = System.currentTimeMillis() + computeTimeout();
      BucketMembershipObserver observer = null;
      boolean needToElectPrimary = true;
      InternalDistributedMember bucketPrimary = null;
      try {
        this.prRegion.checkReadiness();

        Bucket toCreate = this.prRegion.getRegionAdvisor().getBucket(bucketId);

        if (!finishIncompleteCreation) {
          bucketPrimary = this.prRegion.getBucketPrimary(bucketId);
          if (bucketPrimary != null) {
            if (isDebugEnabled) {
              logger.debug(
                  "during atomic creation, discovered that the primary already exists {} returning early",
                  bucketPrimary);
            }
            needToElectPrimary = false;
            return bucketPrimary;
          }
        }

        observer = new BucketMembershipObserver(toCreate).beginMonitoring();
        boolean loggedInsufficentStores = false; // track if insufficient data stores have been
        // detected
        for (;;) {
          this.prRegion.checkReadiness();
          if (this.prRegion.getCache().isCacheAtShutdownAll()) {
            if (isDebugEnabled) {
              logger.debug("Aborted createBucketAtomically due to ShutdownAll");
            }
            throw prRegion.getCache().getCacheClosedException("Cache is shutting down");
          }
          // this.prRegion.getCache().getLogger().config(
          // "DEBUG createBucketAtomically: "
          // + " bucketId=" + this.prRegion.getBucketName(bucketId) +
          // " accepted: " + acceptedMembers +
          // " failed: " + failedMembers);

          long timeLeft = timeOut - System.currentTimeMillis();
          if (timeLeft < 0) {
            // It took too long.
            timedOut(this.prRegion, getAllStores(partitionName), acceptedMembers,
                ALLOCATE_ENOUGH_MEMBERS_TO_HOST_BUCKET.toLocalizedString(), computeTimeout());
            // NOTREACHED
          }
          if (isDebugEnabled) {
            logger.debug("createBucketAtomically: have {} ms left to finish this", timeLeft);
          }

          // Always go back to the advisor, see if any fresh data stores are
          // present.
          Set<InternalDistributedMember> allStores = getAllStores(partitionName);

          loggedInsufficentStores = checkSufficientStores(allStores, loggedInsufficentStores);

          InternalDistributedMember candidate = createBucketInstance(bucketId, newBucketSize,
              excludedMembers, acceptedMembers, failedMembers, timeOut, allStores);
          if (candidate != null) {
            if (this.prRegion.getDistributionManager().enforceUniqueZone()) {
              // enforceUniqueZone property has no effect for a loner. Fix for defect #47181
              if (!(this.prRegion.getDistributionManager() instanceof LonerDistributionManager)) {
                Set<InternalDistributedMember> exm = getBuddyMembersInZone(candidate, allStores);
                exm.remove(candidate);
                exm.removeAll(acceptedMembers);
                excludedMembers.addAll(exm);
              } else {
                // log a warning if Loner
                logger.warn(LocalizedMessage.create(
                    LocalizedStrings.GemFireCache_ENFORCE_UNIQUE_HOST_NOT_APPLICABLE_FOR_LONER));
              }
            }
          }

          // Get an updated list of bucket owners, which should include
          // buckets created concurrently with this createBucketAtomically call
          acceptedMembers = prRegion.getRegionAdvisor().getBucketOwners(bucketId);

          if (isDebugEnabled) {
            logger.debug("Accepted members: {}", acceptedMembers);
          }

          // [sumedh] set the primary as the candidate in the first iteration if
          // the candidate has accepted
          if (bucketPrimary == null && acceptedMembers.contains(candidate)) {
            bucketPrimary = candidate;
          }

          // prune out the stores that have left
          verifyBucketNodes(excludedMembers, partitionName);

          // Note - we used to wait for the created bucket to become primary here
          // if this is a colocated region. We no longer need to do that, because
          // the EndBucketMessage is sent out after bucket creation completes to
          // select the primary.

          // Have we exhausted all candidates?
          final int potentialCandidateCount = (allStores.size()
              - (excludedMembers.size() + acceptedMembers.size() + failedMembers.size()));
          // Determining exhausted members competes with bucket balancing; it's
          // important to re-visit all failed members since "failed" set may
          // contain datastores which at the moment are imbalanced, but yet could
          // be candidates. If the failed members list is empty, its expected
          // that the next iteration clears the (already empty) list.
          final boolean exhaustedPotentialCandidates =
              failedMembers.wasCleared() && potentialCandidateCount <= 0;
          final boolean redundancySatisfied =
              acceptedMembers.size() > this.prRegion.getRedundantCopies();
          final boolean bucketNotCreated = acceptedMembers.size() == 0;

          if (isDebugEnabled) {
            logger.debug(
                "potentialCandidateCount={}, exhaustedPotentialCandidates={}, redundancySatisfied={}, bucketNotCreated={}",
                potentialCandidateCount, exhaustedPotentialCandidates, redundancySatisfied,
                bucketNotCreated);
          }

          if (bucketNotCreated) {
            // if we haven't managed to create the bucket on any nodes, retry.
            continue;
          }

          if (exhaustedPotentialCandidates && !redundancySatisfied) {
            insufficientStores(allStores, acceptedMembers, true);
          }

          // Allow the thread to potentially finish bucket creation even if redundancy was not met.
          // Fix for bug 39283
          if (redundancySatisfied || exhaustedPotentialCandidates) {
            // Tell one of the members to become primary.
            // The rest of the members will be allowed to
            // volunteer for primary.
            endBucketCreation(bucketId, acceptedMembers, bucketPrimary, partitionName);

            final int expectedRemoteHosts = acceptedMembers.size()
                - (acceptedMembers.contains(this.prRegion.getMyId()) ? 1 : 0);
            boolean interrupted = Thread.interrupted();
            try {
              BucketMembershipObserverResults results = observer
                  .waitForOwnersGetPrimary(expectedRemoteHosts, acceptedMembers, partitionName);
              if (results.problematicDeparture) {
                // BZZZT! Member left. Start over.
                continue;
              }
              bucketPrimary = results.primary;
            } catch (InterruptedException e) {
              interrupted = true;
              this.prRegion.getCancelCriterion().checkCancelInProgress(e);
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
            needToElectPrimary = false;
            return bucketPrimary;
          } // almost done
        } // for
      } catch (CancelException e) {
        // Fix for 43544 - We don't need to elect a primary
        // if the cache was closed. The other members will
        // take care of it. This ensures we don't compromise
        // redundancy.
        needToElectPrimary = false;
        throw e;
      } catch (RegionDestroyedException e) {
        // Fix for 43544 - We don't need to elect a primary
        // if the region was destroyed. The other members will
        // take care of it. This ensures we don't compromise
        // redundancy.
        needToElectPrimary = false;
        throw e;
      } catch (PartitionOfflineException e) {
        throw e;
      } catch (RuntimeException e) {
        if (isDebugEnabled) {
          logger.debug("Unable to create new bucket {}: {}", bucketId, e.getMessage(), e);
        }

        // If we're finishing an incomplete bucket creation, don't blast out
        // another message to peers to do so.
        // TODO - should we ignore a PartitionRegionStorageException, rather
        // than reattempting on other nodes?
        if (!finishIncompleteCreation) {
          cleanUpBucket(bucketId);
        }
        throw e;
      } finally {
        if (observer != null) {
          observer.stopMonitoring();
        }
        // Try to make sure everyone that created the bucket can volunteer for primary
        if (needToElectPrimary) {
          try {
            endBucketCreation(bucketId, prRegion.getRegionAdvisor().getBucketOwners(bucketId),
                bucketPrimary, partitionName);
          } catch (Exception e) {
            // if region is going down, then no warning level logs
            if (e instanceof CancelException || e instanceof CacheClosedException
                || (prRegion.getCancelCriterion().isCancelInProgress())) {
              logger.debug("Exception trying choose a primary after bucket creation failure", e);
            } else {
              logger.warn("Exception trying choose a primary after bucket creation failure", e);
            }
          }
        }
      }
    } // synchronized(this)
  }

  /**
   * Figure out which member should be primary for a bucket among the members that have created the
   * bucket, and tell that member to become the primary.
   *
   * @param acceptedMembers The members that now host the bucket
   */
  private void endBucketCreation(int bucketId,
      Collection<InternalDistributedMember> acceptedMembers,
      InternalDistributedMember targetPrimary, String partitionName) {
    if (acceptedMembers.isEmpty()) {
      return;
    }
    acceptedMembers = new HashSet<InternalDistributedMember>(acceptedMembers);

    // TODO prpersist - we need to factor out a method that just chooses
    // the primary. But this will do the trick for the moment.

    // This is for FPR, for a given bucket id , make sure that for given bucket
    // id , only the datastore on which primary partition is defined for this
    // bucket becomes the primary. If primary partition is not available then
    // secondary partition will become primary
    if (partitionName != null) {
      if (isLocalPrimary(partitionName)) {
        targetPrimary = this.prRegion.getMyId();
      } else {
        targetPrimary =
            this.prRegion.getRegionAdvisor().adviseFixedPrimaryPartitionDataStore(bucketId);
        if (targetPrimary == null) {
          Set<InternalDistributedMember> fpDataStores = getFixedPartitionStores(partitionName);
          targetPrimary = fpDataStores.iterator().next();
        }
      }
    }
    if (targetPrimary == null) {
      // [sumedh] we need to select the same primary as chosen earlier (e.g.
      // the parent's in case of colocation) so it is now passed
      // InternalDistributedMember targetPrimary = getPreferredDataStore(
      // acceptedMembers, Collections.<InternalDistributedMember> emptySet());
      targetPrimary =
          getPreferredDataStore(acceptedMembers, Collections.<InternalDistributedMember>emptySet());
    }
    boolean isHosting = acceptedMembers.remove(prRegion.getDistributionManager().getId());
    EndBucketCreationMessage.send(acceptedMembers, targetPrimary, this.prRegion, bucketId);

    // Observer for testing purpose
    final EndBucketCreationObserver observer = testEndObserverInstance;
    if (observer != null) {
      observer.afterEndBucketCreationMessageSend(this.prRegion, bucketId);
    }

    if (isHosting) {
      endBucketCreationLocally(bucketId, targetPrimary);
    }

    if (observer != null) {
      observer.afterEndBucketCreation(this.prRegion, bucketId);
    }
  }

  private boolean isLocalPrimary(String partitionName) {
    List<FixedPartitionAttributesImpl> FPAs = this.prRegion.getFixedPartitionAttributesImpl();
    if (FPAs != null) {
      for (FixedPartitionAttributesImpl fpa : FPAs) {
        if (fpa.getPartitionName().equals(partitionName) && fpa.isPrimary()) {
          return true;
        }
      }
    }
    return false;
  }

  private static volatile EndBucketCreationObserver testEndObserverInstance;

  // Observer for testing purpose
  public static void setTestEndBucketCreationObserver(EndBucketCreationObserver observer) {
    testEndObserverInstance = observer;
  }

  /**
   * Test observer to help reproduce #42429.
   */
  public interface EndBucketCreationObserver {

    void afterEndBucketCreationMessageSend(PartitionedRegion pr, int bucketId);

    void afterEndBucketCreation(PartitionedRegion pr, int bucketId);
  }

  public void endBucketCreationLocally(int bucketId, InternalDistributedMember newPrimary) {

    // Don't elect ourselves as primary or tell others to persist our ID if this member
    // has been destroyed.
    if (prRegion.getCancelCriterion().isCancelInProgress() || prRegion.isDestroyed()) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("endBucketCreationLocally: for region {} bucketId={} new primary: {}",
          this.prRegion.getFullPath(), bucketId, newPrimary);
    }
    BucketAdvisor bucketAdvisor = prRegion.getRegionAdvisor().getBucketAdvisor(bucketId);

    final ProxyBucketRegion proxyBucketRegion = bucketAdvisor.getProxyBucketRegion();
    BucketPersistenceAdvisor persistentAdvisor = proxyBucketRegion.getPersistenceAdvisor();

    // prevent multiple threads from ending bucket creation at the same time.
    // This fixes an issue with 41336, where multiple threads were calling endBucketCreation
    // on the persistent advisor and marking a bucket as initialized twice.
    synchronized (proxyBucketRegion) {
      if (persistentAdvisor != null) {
        BucketRegion realBucket = proxyBucketRegion.getCreatedBucketRegion();
        if (realBucket != null) {
          PersistentMemberID persistentID = realBucket.getPersistentID();
          persistentAdvisor.endBucketCreation(persistentID);
        }
      }

      // We've received an endBucketCreationMessage, but the primary
      // may not have. So now we wait for the chosen member to become
      // primary.
      bucketAdvisor.setPrimaryElector(newPrimary);

      if (prRegion.getGemFireCache().getMyId().equals(newPrimary)) {
        // If we're the choosen primary, volunteer for primary now
        if (bucketAdvisor.isHosting()) {
          bucketAdvisor.clearPrimaryElector();
          bucketAdvisor.volunteerForPrimary();
        }
      } else {
        // It's possible the chosen primary has already left. In
        // that case, volunteer for primary now.
        if (!bucketAdvisor.adviseInitialized().contains(newPrimary)) {
          bucketAdvisor.clearPrimaryElector();
          bucketAdvisor.volunteerForPrimary();
        }

        // If the bucket has had a primary, that means the
        // chosen bucket was primary for a while. Go ahead and
        // clear the primary elector field.
        if (bucketAdvisor.getHadPrimary()) {
          bucketAdvisor.clearPrimaryElector();
          bucketAdvisor.volunteerForPrimary();
        }
      }
    }

    // send out a profile update to indicate the persistence is initialized, if needed.
    if (persistentAdvisor != null) {
      bucketAdvisor.endBucketCreation();
    }

    List<PartitionedRegion> colocatedWithList = ColocationHelper.getColocatedChildRegions(prRegion);
    for (PartitionedRegion child : colocatedWithList) {
      if (child.getRegionAdvisor().isBucketLocal(bucketId)) {
        child.getRedundancyProvider().endBucketCreationLocally(bucketId, newPrimary);
      }
    }
  }

  /**
   * Get buddy data stores on the same Host as the accepted member
   *
   * @return set of members on the same host, not including accepted member
   * @since GemFire 5.9
   *
   */
  private Set<InternalDistributedMember> getBuddyMembersInZone(
      final InternalDistributedMember acceptedMember,
      final Set<InternalDistributedMember> allStores) {
    HashSet<InternalDistributedMember> allMembersOnSystem =
        new HashSet<InternalDistributedMember>();
    DistributionManager dm = this.prRegion.getDistributionManager();
    Set<InternalDistributedMember> buddies = dm.getMembersInSameZone(acceptedMember);
    // TODO Dan - I'm not sure this retain all is necessary, but there may have been a reason we
    // were
    // passing this set in before.
    buddies.retainAll(allStores);
    return buddies;
  }

  /**
   * Early check for resources. This code may be executed for every put operation if there are no
   * datastores present, limit excessive logging.
   *
   * @since GemFire 5.8
   */
  private void earlySufficientStoresCheck(String partitionName) {
    assert Assert.assertHoldsLock(this, false);
    Set currentStores = getAllStores(partitionName);
    if (currentStores.isEmpty()) {
      if (shouldLogInsufficientStores()) {
        insufficientStores(currentStores, Collections.EMPTY_LIST, true);
      }
      insufficientStores(currentStores, Collections.EMPTY_LIST, false);
    }
  }

  /**
   * Limit the frequency for logging the {@link #INSUFFICIENT_STORES_MSG} message to once per PR
   * after which once every {@link #INSUFFICIENT_LOGGING_THROTTLE_TIME} second
   *
   * @return true if it's time to log
   * @since GemFire 5.8
   */
  private boolean shouldLogInsufficientStores() {
    long now = NanoTimer.getTime();
    long delta = now - insufficientLogTimeStamp.get();
    if (this.firstInsufficentStoresLogged.compareAndSet(false, true)
        || delta >= INSUFFICIENT_LOGGING_THROTTLE_TIME) {
      insufficientLogTimeStamp.set(now);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Compute timeout for waiting for a bucket. Prefer
   * {@link #DATASTORE_DISCOVERY_TIMEOUT_MILLISECONDS} over
   * {@link PartitionedRegion#getRetryTimeout()}
   *
   * @return the milliseconds to wait for a bucket creation operation
   */
  private long computeTimeout() {
    if (DATASTORE_DISCOVERY_TIMEOUT_MILLISECONDS != null) {
      long millis = DATASTORE_DISCOVERY_TIMEOUT_MILLISECONDS.longValue();
      if (millis > 0) { // only positive values allowed
        return millis;
      }
    }
    return this.prRegion.getRetryTimeout();
  }

  /**
   * Check to determine that there are enough datastore VMs to start the bucket creation processes.
   * Log a warning or throw an exception indicating when there are not enough datastore VMs.
   *
   * @param allStores All known data store instances (including local)
   * @param loggedInsufficentStores indicates whether a warning has been logged
   * @return true when a warning has been logged, false if a warning should be logged.
   */
  private boolean checkSufficientStores(final Set allStores,
      final boolean loggedInsufficentStores) {
    // Report (only once) if insufficient data store have been detected.
    if (!loggedInsufficentStores) {
      if (allStores.size() == 0) {
        insufficientStores(allStores, Collections.EMPTY_LIST, true);
        return true;
      }
    } else {
      if (allStores.size() > 0) {
        // Excellent, sufficient resources were found!
        final StringId logStr =
            LocalizedStrings.PRHARRedundancyProvider_0_IN_THE_PARTITIONED_REGION_REGION_NAME_1;
        final Object[] logArgs =
            new Object[] {SUFFICIENT_STORES_MSG.toLocalizedString(), prRegion.getFullPath()};
        if (TEST_MODE) {
          logger.fatal(LocalizedMessage.create(logStr, logArgs));
        } else {
          logger.info(LocalizedMessage.create(logStr, logArgs));
        }
        return false;
      } else {
        // Already logged warning, there are no datastores
        insufficientStores(allStores, Collections.EMPTY_LIST, false);
        // UNREACHABLE
      }
    }
    return loggedInsufficentStores;
  }

  /**
   * Clean up locally created bucket and tell other VMs to attempt recovering redundancy
   *
   * @param buck the bucket identifier
   */
  private void cleanUpBucket(int buck) {
    Set dataStores = this.prRegion.getRegionAdvisor().adviseDataStore();
    BucketBackupMessage.send(dataStores, this.prRegion, buck);
  }

  public void finishIncompleteBucketCreation(int bucketId) {
    String partitionName = null;
    final long startTime = PartitionedRegionStats.startTime();
    if (this.prRegion.isFixedPartitionedRegion()) {
      FixedPartitionAttributesImpl fpa =
          PartitionedRegionHelper.getFixedPartitionAttributesForBucket(this.prRegion, bucketId);
      partitionName = fpa.getPartitionName();
    }
    createBucketAtomically(bucketId, 0, startTime, true, partitionName);
  }

  /**
   * Creates bucket with ID bucketId on targetNode. This method will also create the bucket for all
   * of the child colocated PRs.
   *
   * @param isRebalance true if bucket creation is directed by rebalancing
   * @return true if the bucket was sucessfully created
   */
  public boolean createBackupBucketOnMember(final int bucketId,
      final InternalDistributedMember targetNMember, final boolean isRebalance,
      boolean replaceOfflineData, InternalDistributedMember moveSource, boolean forceCreation) {

    if (logger.isDebugEnabled()) {
      logger.debug("createBackupBucketOnMember for bucketId={} member: {}",
          this.prRegion.bucketStringForLogs(bucketId), targetNMember);
    }

    if (!(targetNMember.equals(this.prRegion.getMyId()))) {
      // final StoppableReentrantReadWriteLock.StoppableReadLock isClosingReadLock;
      PartitionProfile pp = this.prRegion.getRegionAdvisor().getPartitionProfile(targetNMember);
      if (pp != null) {
        // isClosingReadLock = pp.getIsClosingReadLock(
        // this.prRegion.getCancelCriterion());
      } else {
        return false;
      }

      try {
        ManageBackupBucketMessage.NodeResponse response =
            ManageBackupBucketMessage.send(targetNMember, this.prRegion, bucketId, isRebalance,
                replaceOfflineData, moveSource, forceCreation);

        if (response.waitForAcceptance()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "createBackupBucketOnMember: Bucket creation succeed for bucketId={} on member = {}",
                this.prRegion.bucketStringForLogs(bucketId), targetNMember);
          }

          return true;
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "createBackupBucketOnMember: Bucket creation failed for bucketId={} on member = {}",
                this.prRegion.bucketStringForLogs(bucketId), targetNMember);
          }

          return false;
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        if (e instanceof ForceReattemptException) {
          // no log needed see bug 37569
        } else if (e instanceof CancelException
            || (e.getCause() != null && (e.getCause() instanceof CancelException))) {
          // no need to log exceptions caused by cache closure
        } else {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.PRHARedundancyProvider_EXCEPTION_CREATING_PARTITION_ON__0,
              targetNMember), e);
        }
        return false;
      }
    } else {
      final PartitionedRegionDataStore prDS = this.prRegion.getDataStore();
      boolean bucketManaged = prDS != null && prDS.grabBucket(bucketId, moveSource, forceCreation,
          replaceOfflineData, isRebalance, null, false).equals(CreateBucketResult.CREATED);
      if (!bucketManaged) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "createBackupBucketOnMember: Local data store refused to accommodate the data for bucketId={} prDS={}",
              this.prRegion.bucketStringForLogs(bucketId), prDS);
        }
      }
      return bucketManaged;
    }
  }

  private static final ThreadLocal forceLocalPrimaries = new ThreadLocal();

  public static void setForceLocalPrimaries(boolean v) {
    forceLocalPrimaries.set(Boolean.valueOf(v));
  }

  private boolean getForceLocalPrimaries() {
    boolean result = false;
    Boolean v = (Boolean) forceLocalPrimaries.get();
    if (v != null) {
      result = v.booleanValue();
    }
    return result;
  }

  /**
   * Creates bucket with ID bucketId on targetNode.
   *
   * @param forceCreation inform the targetMember it must attempt host the bucket, appropriately
   *        ignoring it's maximums
   * @return a response object
   */
  public ManageBucketRsp createBucketOnMember(final int bucketId,
      final InternalDistributedMember targetNMember, final int newBucketSize,
      boolean forceCreation) {
    if (logger.isDebugEnabled()) {
      logger.debug("createBucketOnMember for bucketId={} member: {}{}",
          this.prRegion.bucketStringForLogs(bucketId), targetNMember,
          (forceCreation ? " forced" : ""));
    }

    if (!(targetNMember.equals(this.prRegion.getMyId()))) {
      // final StoppableReentrantReadWriteLock.StoppableReadLock isClosingReadLock;
      PartitionProfile pp = this.prRegion.getRegionAdvisor().getPartitionProfile(targetNMember);
      if (pp != null) {
        // isClosingReadLock = pp.getIsClosingReadLock(
        // this.prRegion.getCancelCriterion());
      } else {
        return ManageBucketRsp.NO;
      }

      try {
        // isClosingReadLock.lock(); // Grab the read lock, preventing any region closures
        // on this remote Node until this bucket is fully published, forcing the closing
        // Node to recognize any pre-natal buckets.
        NodeResponse response = ManageBucketMessage.send(targetNMember, this.prRegion, bucketId,
            newBucketSize, forceCreation);

        if (response.waitForAcceptance()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "createBucketOnMember: Bucket creation succeed for bucketId={} on member = {}",
                this.prRegion.bucketStringForLogs(bucketId), targetNMember);
          }

          // lockList.add(isClosingReadLock);
          return ManageBucketRsp.YES;
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "createBucketOnMember: Bucket creation failed for bucketId={} on member = {}",
                this.prRegion.bucketStringForLogs(bucketId), targetNMember);
          }

          // isClosingReadLock.unlock();
          return response.rejectedDueToInitialization() ? ManageBucketRsp.NO_INITIALIZING
              : ManageBucketRsp.NO;
        }
      } catch (PartitionOfflineException e) {
        throw e;
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        if (e instanceof CancelException
            || (e.getCause() != null && (e.getCause() instanceof CancelException))) {
          // no need to log exceptions caused by cache closure
          return ManageBucketRsp.CLOSED;
        } else if (e instanceof ForceReattemptException) {
          // no log needed see bug 37569
        } else {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.PRHARedundancyProvider_EXCEPTION_CREATING_PARTITION_ON__0,
              targetNMember), e);
        }
        // isClosingReadLock.unlock();
        return ManageBucketRsp.NO;
      }
    } else {
      final PartitionedRegionDataStore prDS = this.prRegion.getDataStore();
      boolean bucketManaged = prDS != null && prDS.handleManageBucketRequest(bucketId,
          newBucketSize, this.prRegion.getMyId(), forceCreation);
      if (!bucketManaged) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "createBucketOnMember: Local data store not able to accommodate the data for bucketId={}",
              this.prRegion.bucketStringForLogs(bucketId));
        }
      }
      return ManageBucketRsp.valueOf(bucketManaged);
    }
  }

  /**
   * Select the member with which is hosting the same bucketid for the PR it is colocated with In
   * case of primary it returns the same node whereas in case of secondary it will return the least
   * loaded datastore which is hosting the bucketid.
   *
   * @return InternalDistributedMember colocated data store
   * @since GemFire 5.8Beta
   */
  private InternalDistributedMember getColocatedDataStore(
      Collection<InternalDistributedMember> candidates,
      Collection<InternalDistributedMember> alreadyUsed, int bucketId, String prName) {
    Assert.assertTrue(prName != null); // precondition1
    PartitionedRegion colocatedRegion = ColocationHelper.getColocatedRegion(this.prRegion);
    Region prRoot = PartitionedRegionHelper.getPRRoot(prRegion.getCache());
    PartitionRegionConfig config =
        (PartitionRegionConfig) prRoot.get(prRegion.getRegionIdentifier());
    if (!config.isColocationComplete()) {
      throw new IllegalStateException("Cannot create buckets, as colocated regions are not "
          + "configured to be at the same nodes.");
    }

    RegionAdvisor advisor = colocatedRegion.getRegionAdvisor();
    if (alreadyUsed.isEmpty()) {
      InternalDistributedMember primary = advisor.getPrimaryMemberForBucket(bucketId);
      if (!candidates.contains(primary)) {
        return null;
      }
      return primary;
    }
    Set bucketOwnersSet = advisor.getBucketOwners(bucketId);
    bucketOwnersSet.retainAll(candidates);
    ArrayList members = new ArrayList(bucketOwnersSet);
    if (members.isEmpty()) {
      return null;
    }
    return getPreferredDataStore(members, alreadyUsed);
  }

  /**
   * Select the member with the fewest buckets, among those with the fewest randomly select one.
   *
   * Under concurrent access, the data that this method uses, may be somewhat volatile, note that
   * createBucketAtomically synchronizes to enhance the consistency of the data used in this method.
   *
   * @param candidates ArrayList of InternalDistributedMember, potential datastores
   * @param alreadyUsed data stores already in use
   * @return a member with the fewest buckets or null if no datastores
   */
  private InternalDistributedMember getPreferredDataStore(
      Collection<InternalDistributedMember> candidates,
      final Collection<InternalDistributedMember> alreadyUsed) {
    /* has a primary already been chosen? */
    final boolean forPrimary = alreadyUsed.size() == 0;

    if (forPrimary && getForceLocalPrimaries()) {
      PartitionedRegionDataStore myDS = this.prRegion.getDataStore();
      if (myDS != null) {
        return this.prRegion.getMyId();
      }
    }

    if (candidates.size() == 1) {
      return candidates.iterator().next();
    }
    Assert.assertTrue(candidates.size() > 1);

    // Convert peers to DataStoreBuckets
    ArrayList<DataStoreBuckets> stores = this.prRegion.getRegionAdvisor()
        .adviseFilteredDataStores(new HashSet<InternalDistributedMember>(candidates));

    final DistributionManager dm = this.prRegion.getDistributionManager();
    // Add ourself as a candidate, if appropriate
    InternalDistributedMember moi = dm.getId();
    PartitionedRegionDataStore myDS = this.prRegion.getDataStore();
    if (myDS != null && candidates.contains(moi)) {
      int bucketCount = myDS.getBucketsManaged();
      int priCount = myDS.getNumberOfPrimaryBucketsManaged();
      int localMaxMemory = this.prRegion.getLocalMaxMemory();
      stores.add(new DataStoreBuckets(moi, bucketCount, priCount, localMaxMemory));
    }
    if (stores.isEmpty()) {
      return null;
    }

    // ---------------------------------------------
    // Calculate all hosts who already have this bucket
    final HashSet<InternalDistributedMember> existingHosts =
        new HashSet<InternalDistributedMember>();
    Iterator<InternalDistributedMember> it = alreadyUsed.iterator();
    while (it.hasNext()) {
      InternalDistributedMember mem = it.next();
      existingHosts.addAll(dm.getMembersInSameZone(mem));
    }

    Comparator<DataStoreBuckets> comparator = new Comparator<DataStoreBuckets>() {
      public int compare(DataStoreBuckets d1, DataStoreBuckets d2) {
        boolean host1Used = existingHosts.contains(d1.memberId);
        boolean host2Used = existingHosts.contains(d2.memberId);

        if (!host1Used && host2Used) {
          return -1; // host1 preferred
        }
        if (host1Used && !host2Used) {
          return 1; // host2 preferred
        }

        // Six eggs, half a dozen. Look for least loaded.
        float metric1, metric2;
        if (forPrimary) {
          metric1 = d1.numPrimaries / (float) d1.localMaxMemoryMB;
          metric2 = d2.numPrimaries / (float) d2.localMaxMemoryMB;
        } else {
          metric1 = d1.numBuckets / (float) d1.localMaxMemoryMB;
          metric2 = d2.numBuckets / (float) d2.localMaxMemoryMB;
        }
        int result = Float.compare(metric1, metric2);
        if (result == 0) {
          // if they have the same load, choose the member with the
          // higher localMaxMemory
          result = d2.localMaxMemoryMB - d1.localMaxMemoryMB;
        }
        return result;
      }
    };

    // ---------------------------------------------
    // First step is to sort datastores first by those whose hosts don't
    // hold this bucket, and then secondarily by loading.
    Collections.sort(stores, comparator);
    if (logger.isDebugEnabled()) {
      logger.debug(fancyFormatBucketAllocation("Sorted ", stores, existingHosts));
    }

    // ---------------------------------------------
    // Always add the first datastore and note just how good it is.
    DataStoreBuckets bestDataStore = stores.get(0);
    ArrayList<DataStoreBuckets> bestStores = new ArrayList<DataStoreBuckets>();
    bestStores.add(bestDataStore);

    final boolean allStoresInUse = alreadyUsed.contains(bestDataStore.memberId);

    // ---------------------------------------------
    // Collect all of the other hosts in this sorted list that are as good
    // as the very first one.
    for (int i = 1; i < stores.size(); i++) {
      DataStoreBuckets aDataStore = stores.get(i);
      if (!allStoresInUse && alreadyUsed.contains(aDataStore.memberId)) {
        // Only choose between the ones not in use.
        break;
      }

      if (comparator.compare(bestDataStore, aDataStore) != 0) {
        break;
      }
      bestStores.add(aDataStore);
    }
    if (logger.isDebugEnabled()) {
      logger.debug(fancyFormatBucketAllocation("Best Stores ", bestStores, existingHosts));
    }

    // ---------------------------------------------
    int chosen;
    if (DISABLE_CREATE_BUCKET_RANDOMNESS) {
      chosen = 0;
    } else {
      // Pick one (at random)
      chosen = PartitionedRegion.RANDOM.nextInt(bestStores.size());
    }
    DataStoreBuckets aDataStore = bestStores.get(chosen);
    return aDataStore.memberId;
  }

  /**
   * Adds a membership listener to watch for member departures, and schedules a task to recover
   * redundancy of existing buckets
   */
  public void startRedundancyRecovery() {
    prRegion.getRegionAdvisor().addMembershipListener(new PRMembershipListener());
    scheduleRedundancyRecovery(null);
  }

  /**
   * Log bucket allocation in the log files in this format:
   *
   * <pre>
   * member1: +5/20
   * member2: -10/5
   * </pre>
   *
   * After the member name, the +/- indicates whether or not this bucket is already hosted on the
   * given member. This is followed by the number of hosted primaries followed by the number of
   * hosted non-primary buckets.
   *
   * @param prefix first part of message to print
   * @param dataStores list of stores
   * @param existingStores to mark those already in use
   */
  private String fancyFormatBucketAllocation(String prefix, List dataStores, Set existingStores) {
    StringBuffer logStr = new StringBuffer();
    if (prefix != null) {
      logStr.append(prefix);
    }
    logStr.append("Bucket Allocation for prId=" + this.prRegion.getPRId() + ":\n");
    for (Iterator i = dataStores.iterator(); i.hasNext();) {
      DataStoreBuckets dsb = (DataStoreBuckets) i.next();
      logStr.append(dsb.memberId).append(": ");
      if (existingStores.contains(dsb.memberId)) {
        logStr.append("+");
      } else {
        logStr.append("-");
      }
      logStr.append(Integer.toString(dsb.numPrimaries));
      logStr.append("/");
      logStr.append(Integer.toString(dsb.numBuckets - dsb.numPrimaries));
      // for (int j = 0; j < dsb.numPrimaries; j++) {
      // logStr.append('#');
      // }
      // int nonPrimary = dsb.numBuckets - dsb.numPrimaries;
      // for (int j = 0; j < nonPrimary; j++) {
      // logStr.append('*');
      // }
      logStr.append('\n');
    }
    return logStr.toString();
  }

  public static class DataStoreBuckets {
    public final InternalDistributedMember memberId;
    public final int numBuckets;
    public final int numPrimaries;
    private final int localMaxMemoryMB;

    public DataStoreBuckets(InternalDistributedMember mem, int buckets, int primaryBuckets,
        int localMaxMemory) {
      this.memberId = mem;
      this.numBuckets = buckets;
      this.numPrimaries = primaryBuckets;
      this.localMaxMemoryMB = localMaxMemory;
    }

    @Override
    public boolean equals(Object obj) {
      if ((obj == null) || !(obj instanceof DataStoreBuckets)) {
        return false;
      }
      DataStoreBuckets other = (DataStoreBuckets) obj;
      return this.numBuckets == other.numBuckets && this.memberId.equals(other.memberId);
    }

    @Override
    public int hashCode() {
      return this.memberId.hashCode();
    }

    @Override
    public String toString() {
      return "DataStoreBuckets memberId=" + this.memberId + "; numBuckets=" + this.numBuckets
          + "; numPrimaries=" + this.numPrimaries;
    }
  }

  /**
   * Verifies the members and removes the members that are either not present in the
   * DistributedSystem or are no longer part of the PartitionedRegion (close/localDestroy has been
   * performed.) .
   *
   * @param members collection of members to scan and modify
   */
  void verifyBucketNodes(Collection<InternalDistributedMember> members, String partitionName) {
    if (members == null || members.isEmpty()) {
      return;
    }

    // Revisit region advisor, get current bucket stores.
    final Set<InternalDistributedMember> availableMembers = getAllStores(partitionName);

    // boolean debugAnyRemoved = false;
    for (Iterator<InternalDistributedMember> itr = members.iterator(); itr.hasNext();) {
      InternalDistributedMember node = itr.next();
      if (!availableMembers.contains(node)) {
        if (logger.isDebugEnabled()) {
          logger.debug("verifyBucketNodes: removing member {}", node);
          // debugAnyRemoved = true;
        }
        itr.remove();
        Assert.assertTrue(!members.contains(node), "return value does not contain " + node);
      }
    } // for
  }

  /**
   * Schedule a task to perform redundancy recovery for a new node or for the node departed.
   */
  public void scheduleRedundancyRecovery(Object failedMemId) {

    final boolean isStartup = failedMemId == null ? true : false;
    final InternalCache cache = this.prRegion.getCache();
    final int redundantCopies = PRHARedundancyProvider.this.prRegion.getRedundantCopies();
    final long delay;
    final boolean movePrimaries;

    if (isStartup) {
      delay = this.prRegion.getPartitionAttributes().getStartupRecoveryDelay();
      movePrimaries = !Boolean
          .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_MOVE_PRIMARIES_ON_STARTUP");
    } else {
      delay = this.prRegion.getPartitionAttributes().getRecoveryDelay();
      movePrimaries = false;
    }
    final boolean requiresRedundancyRecovery = delay >= 0;

    if (!requiresRedundancyRecovery) {
      return;
    }
    if (!PRHARedundancyProvider.this.prRegion.isDataStore()) {
      return;
    }
    Runnable task = new RecoveryRunnable(this) {
      @Override
      public void run2() {
        try {
          final boolean isFixedPartitionedRegion =
              PRHARedundancyProvider.this.prRegion.isFixedPartitionedRegion();


          // Fix for 43582 - always replace offline data for fixed partitioned
          // regions - this guarantees we create the buckets we are supposed to
          // create on this node.
          boolean replaceOfflineData = isFixedPartitionedRegion || !isStartup;

          RebalanceDirector director;
          if (isFixedPartitionedRegion) {
            director = new FPRDirector(true, movePrimaries);
          } else {
            director = new CompositeDirector(true, true, false, movePrimaries);
          }

          final PartitionedRegionRebalanceOp rebalance = new PartitionedRegionRebalanceOp(
              PRHARedundancyProvider.this.prRegion, false, director, replaceOfflineData, false);

          long start = PRHARedundancyProvider.this.prRegion.getPrStats().startRecovery();

          if (isFixedPartitionedRegion) {
            rebalance.executeFPA();
          } else {
            rebalance.execute();
          }

          PRHARedundancyProvider.this.prRegion.getPrStats().endRecovery(start);
          PRHARedundancyProvider.this.recoveryFuture = null;
        } catch (CancelException e) {
          logger.debug("Cache closed while recovery in progress");
        } catch (RegionDestroyedException e) {
          logger.debug("Region destroyed while recovery in progress");
        } catch (Exception e) {
          logger.error(
              LocalizedMessage.create(
                  LocalizedStrings.PRHARedundancyProvider_UNEXPECTED_EXCEPTION_DURING_BUCKET_RECOVERY),
              e);
        }
      }
    };

    synchronized (this.shutdownLock) { // possible fix for bug 41094
      if (!this.shutdown) {
        try {
          if (logger.isDebugEnabled()) {
            if (isStartup) {
              logger.debug(this.prRegion + " scheduling redundancy recovery in {} ms", delay);
            } else {
              logger.debug(
                  "prRegion scheduling redundancy recovery after departure/crash/error in {} in {} ms",
                  failedMemId, delay);
            }
          }
          recoveryFuture = this.recoveryExecutor.schedule(task, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
          // ok, the executor is shutting down.
        }
      }
    }
  }

  public boolean isRedundancyImpaired() {
    int numBuckets = this.prRegion.getPartitionAttributes().getTotalNumBuckets();
    int targetRedundancy = this.prRegion.getPartitionAttributes().getRedundantCopies();

    for (int i = 0; i < numBuckets; i++) {
      int redundancy = this.prRegion.getRegionAdvisor().getBucketRedundancy(i);
      if (redundancy < targetRedundancy && redundancy != -1 || redundancy > targetRedundancy) {
        return true;
      }
    }

    return false;
  }

  public boolean recoverPersistentBuckets() {

    /**
     * To handle a case where ParallelGatewaySender is persistent but userPR is not First recover
     * the GatewaySender buckets for ParallelGatewaySender irrespective of whether colocation is
     * complete or not.
     */
    PartitionedRegion leaderRegion = ColocationHelper.getLeaderRegion(this.prRegion);


    // Check if the leader region or some child shadow PR region is persistent
    // and return the first persistent region found
    PartitionedRegion persistentLeader = getPersistentLeader();

    // If there is no persistent region in the colocation chain, no need to recover.
    if (persistentLeader == null) {
      return true;
    }

    if (!ColocationHelper.checkMembersColocation(leaderRegion,
        leaderRegion.getDistributionManager().getDistributionManagerId())) {
      if (logger.isDebugEnabled()) {
        logger.debug("Skipping persistent recovery of {} because colocation is not complete for {}",
            prRegion, leaderRegion);
      }
      return false;
    }

    // TODO prpersist - It would make sense to hold the lock here in some cases
    // to prevent confusing members that are trying to rebalance. BUT, these persistent regions
    // need to wait for other members to recover during initialization.
    // RecoveryLock lock = leaderRegion.getRecoveryLock();
    // lock.lock();
    // try {
    final ProxyBucketRegion[] proxyBucketArray =
        persistentLeader.getRegionAdvisor().getProxyBucketArray();

    for (ProxyBucketRegion proxyBucket : proxyBucketArray) {
      proxyBucket.initializePersistenceAdvisor();
    }
    Set<InternalDistributedMember> peers = this.prRegion.getRegionAdvisor().adviseGeneric();

    // TODO prpersist - Ok, this is super lame. We need to make sure here that we don't run into
    // this race condition
    // 1) We get a membership view from member A
    // 2) Member B removes itself, and distributes to us and A. We don't remove B
    // 3) We apply the membership view from A, which includes B.
    // That will add B back into the set.
    // This state flush will make sure that any membership changes
    // That are in progress on the peers are finished.
    MembershipFlushRequest.send(peers, this.prRegion.getDistributionManager(),
        this.prRegion.getFullPath());


    ArrayList<ProxyBucketRegion> bucketsNotHostedLocally =
        new ArrayList<ProxyBucketRegion>(proxyBucketArray.length);
    ArrayList<ProxyBucketRegion> bucketsHostedLocally =
        new ArrayList<ProxyBucketRegion>(proxyBucketArray.length);

    /*
     * Start the redundancy logger before recovering any proxy buckets.
     */
    allBucketsRecoveredFromDisk = new CountDownLatch(proxyBucketArray.length);
    try {
      if (proxyBucketArray.length > 0) {
        this.redundancyLogger = new RedundancyLogger(this);
        Thread loggingThread = new Thread(this.redundancyLogger,
            "RedundancyLogger for region " + this.prRegion.getName());
        loggingThread.start();
      }
    } catch (RuntimeException e) {
      allBucketsRecoveredFromDisk = null;
      throw e;
    }

    /*
     * Spawn a separate thread for bucket that we previously hosted to recover that bucket.
     *
     * That thread will get to the point at which it has determined that at least one member
     * (possibly the local member) has fully initialized the bucket, at which it will count down the
     * someMemberRecoveredLatch latch on the bucket.
     *
     * Once at least one copy of each bucket has been created in the distributed system, the
     * initPRInternals method will exit. Some of the threads spawned here will still be doing GII's
     * in the background. This allows the system to become usable as fast as possible.
     *
     * If we used a bounded thread pool here, we end up waiting for some buckets to finish there GII
     * before returning from initPRInternals. In the future maybe we could let the create bucket
     * return and pass the GII task to a separate thread pool.
     *
     */
    for (final ProxyBucketRegion proxyBucket : proxyBucketArray) {
      if (proxyBucket.getPersistenceAdvisor().wasHosting()) {
        final RecoveryRunnable recoveryRunnable = new RecoveryRunnable(this) {


          @Override
          public void run() {
            // Fix for 44551 - make sure that we always count down
            // this latch, even if the region was destroyed.
            try {
              super.run();
            } finally {
              allBucketsRecoveredFromDisk.countDown();
            }
          }

          @Override
          public void run2() {
            proxyBucket.recoverFromDiskRecursively();
          }
        };
        Thread recoveryThread =
            new Thread(recoveryRunnable, "Recovery thread for bucket " + proxyBucket.getName());
        recoveryThread.start();
        bucketsHostedLocally.add(proxyBucket);
      } else {
        bucketsNotHostedLocally.add(proxyBucket);
      }
    }

    try {
      // Partial fix for 44045, try to recover the local
      // buckets before the proxy buckets. This will allow us
      // to detect any ConflictingDataException before the proxy
      // buckets update their membership view.
      for (final ProxyBucketRegion proxyBucket : bucketsHostedLocally) {
        proxyBucket.waitForPrimaryPersistentRecovery();
      }
      for (final ProxyBucketRegion proxyBucket : bucketsNotHostedLocally) {
        proxyBucket.recoverFromDiskRecursively();
      }
    } finally {
      for (final ProxyBucketRegion proxyBucket : bucketsNotHostedLocally) {
        allBucketsRecoveredFromDisk.countDown();
      }
    }

    return true;
    // } finally {
    // lock.unlock();
    // }
  }

  /**
   * Check to see if any colocated region of the current region is persistent. It's not enough to
   * check just the leader region, because a child region might be a persistent parallel WAN queue,
   * which is allowed.
   *
   * @return the most senior region in the colocation chain (closest to the leader) that is
   *         persistent.
   */
  protected PartitionedRegion getPersistentLeader() {
    PartitionedRegion leader = ColocationHelper.getLeaderRegion(this.prRegion);

    return findPersistentRegionRecursively(leader);
  }

  private PartitionedRegion findPersistentRegionRecursively(PartitionedRegion pr) {
    if (pr.getDataPolicy().withPersistence()) {
      return pr;
    }
    for (PartitionedRegion child : ColocationHelper.getColocatedChildRegions(pr)) {
      PartitionedRegion leader = findPersistentRegionRecursively(child);
      if (leader != null) {
        return leader;
      }
    }
    return null;
  }

  public void scheduleCreateMissingBuckets() {
    if (this.prRegion.getColocatedWith() != null
        && ColocationHelper.isColocationComplete(this.prRegion)) {
      Runnable task = new CreateMissingBucketsTask(this);
      final InternalResourceManager resourceManager =
          this.prRegion.getGemFireCache().getInternalResourceManager();
      resourceManager.getExecutor().execute(task);
    }
  }

  public void shutdown() {
    synchronized (this.shutdownLock) { // possible fix for bug 41094
      this.shutdown = true;
      ScheduledFuture<?> recoveryFuture = this.recoveryFuture;
      if (recoveryFuture != null) {
        recoveryFuture.cancel(false/* mayInterruptIfRunning */);
        this.recoveryExecutor.purge();
      }
    }
  }

  /**
   * Creates and fills in a PartitionMemberDetails for the partitioned region.
   *
   * @param internal true if internal-only details should be included
   * @param loadProbe the LoadProbe to use
   * @return PartitionRegionInfo for the partitioned region
   */
  public InternalPRInfo buildPartitionedRegionInfo(final boolean internal,
      final LoadProbe loadProbe) {

    final PartitionedRegion pr = this.prRegion;

    if (pr == null) {
      return null;
    }

    PartitionedRegionStats prStats = pr.getPrStats();

    int configuredBucketCount = pr.getTotalNumberOfBuckets();
    int createdBucketCount = pr.getRegionAdvisor().getCreatedBucketsCount();
    int lowRedundancyBucketCount = prStats.getLowRedundancyBucketCount();
    int configuredRedundantCopies = pr.getRedundantCopies();
    int actualRedundantCopies = prStats.getActualRedundantCopies();

    final PartitionedRegionDataStore ds = pr.getDataStore();

    Set<InternalDistributedMember> datastores = pr.getRegionAdvisor().adviseDataStore();

    // int size = datastores.size() + (ds == null ? 0 : 1);

    Set<InternalPartitionDetails> memberDetails = new TreeSet<InternalPartitionDetails>();

    OfflineMemberDetails offlineMembers = null;
    boolean fetchOfflineMembers = false;
    if (ds != null) {
      memberDetails.add(buildPartitionMemberDetails(internal, loadProbe));
      offlineMembers = fetchOfflineMembers();
    } else {
      fetchOfflineMembers = true;
    }

    // Get remote results
    if (!datastores.isEmpty()) {
      FetchPartitionDetailsResponse response = FetchPartitionDetailsMessage.send(datastores, pr,
          internal, fetchOfflineMembers, loadProbe);
      memberDetails.addAll(response.waitForResponse());
      if (fetchOfflineMembers) {
        offlineMembers = response.getOfflineMembers();
      }
    }

    String colocatedWithPath = pr.getColocatedWith();



    InternalPRInfo details = new PartitionRegionInfoImpl(pr.getFullPath(), configuredBucketCount,
        createdBucketCount, lowRedundancyBucketCount, configuredRedundantCopies,
        actualRedundantCopies, memberDetails, colocatedWithPath, offlineMembers);

    return details;
  }

  /**
   * Retrieve the set of members which are currently offline for all buckets.
   */
  public OfflineMemberDetailsImpl fetchOfflineMembers() {
    ProxyBucketRegion[] proxyBuckets = prRegion.getRegionAdvisor().getProxyBucketArray();
    Set<PersistentMemberID>[] offlineMembers = new Set[proxyBuckets.length];
    for (int i = 0; i < proxyBuckets.length; i++) {
      ProxyBucketRegion proxy = proxyBuckets[i];
      if (this.prRegion.getDataPolicy().withPersistence()) {
        Set<PersistentMemberID> persistedMembers =
            proxy.getPersistenceAdvisor().getMissingMembers();
        if (persistedMembers == null) {
          persistedMembers = Collections.emptySet();
        }
        offlineMembers[i] = persistedMembers;
      } else {
        offlineMembers[i] = Collections.emptySet();
      }
    }
    return new OfflineMemberDetailsImpl(offlineMembers);
  }

  /**
   * Creates and fills in a PartitionMemberDetails for the local member.
   *
   * @param internal true if internal-only details should be included
   * @param loadProbe the LoadProbe to use
   * @return PartitionMemberDetails for the local member
   */
  public InternalPartitionDetails buildPartitionMemberDetails(final boolean internal,
      final LoadProbe loadProbe) {

    final PartitionedRegion pr = this.prRegion;

    PartitionedRegionDataStore ds = pr.getDataStore();
    if (ds == null) {
      return null;
    }

    InternalPartitionDetails localDetails = null;

    long size = 0;
    InternalDistributedMember localMember = (InternalDistributedMember) pr.getMyId();

    int configuredBucketCount = pr.getTotalNumberOfBuckets();
    long[] bucketSizes = new long[configuredBucketCount];
    // key: bid, value: size
    Map<Integer, Integer> bucketSizeMap = ds.getSizeLocally();
    for (Iterator<Map.Entry<Integer, Integer>> iter = bucketSizeMap.entrySet().iterator(); iter
        .hasNext();) {
      Map.Entry<Integer, Integer> me = iter.next();
      int bid = me.getKey().intValue();
      long bucketSize = ds.getBucketSize(bid);
      bucketSizes[bid] = bucketSize;
      size += bucketSize;
    }

    if (internal) {
      waitForPersistentBucketRecoveryOrClose();


      PRLoad prLoad = loadProbe.getLoad(pr);
      localDetails =
          new PartitionMemberInfoImpl(localMember, pr.getLocalMaxMemory() * (1024L * 1024L), size,
              ds.getBucketsManaged(), ds.getNumberOfPrimaryBucketsManaged(), prLoad, bucketSizes);
    } else {
      localDetails =
          new PartitionMemberInfoImpl(localMember, pr.getLocalMaxMemory() * (1024L * 1024L), size,
              ds.getBucketsManaged(), ds.getNumberOfPrimaryBucketsManaged());
    }
    return localDetails;
  }

  /**
   * Wait for all persistent buckets to be recovered from disk, or for the region to be closed,
   * whichever happens first.
   */
  protected void waitForPersistentBucketRecoveryOrClose() {
    CountDownLatch recoveryLatch = allBucketsRecoveredFromDisk;
    if (recoveryLatch != null) {
      boolean interrupted = false;
      while (true) {
        try {
          this.prRegion.getCancelCriterion().checkCancelInProgress(null);
          boolean done = recoveryLatch.await(
              PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION, TimeUnit.MILLISECONDS);
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

    List<PartitionedRegion> colocatedRegions =
        ColocationHelper.getColocatedChildRegions(this.prRegion);
    for (PartitionedRegion child : colocatedRegions) {
      child.getRedundancyProvider().waitForPersistentBucketRecoveryOrClose();
    }
  }

  /**
   * Wait for all persistent buckets to be recovered from disk, regardless of whether the region is
   * currently being closed.
   */
  protected void waitForPersistentBucketRecovery() {
    CountDownLatch recoveryLatch = allBucketsRecoveredFromDisk;
    if (recoveryLatch != null) {
      boolean interrupted = false;
      while (true) {
        try {
          recoveryLatch.await();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public boolean isPersistentRecoveryComplete() {
    if (!ColocationHelper.checkMembersColocation(this.prRegion, this.prRegion.getMyId())) {
      return false;
    }

    if (allBucketsRecoveredFromDisk != null && allBucketsRecoveredFromDisk.getCount() > 0) {
      return false;
    }

    Map<String, PartitionedRegion> colocatedRegions =
        ColocationHelper.getAllColocationRegions(this.prRegion);

    for (PartitionedRegion region : colocatedRegions.values()) {
      PRHARedundancyProvider redundancyProvider = region.getRedundancyProvider();
      if (redundancyProvider.allBucketsRecoveredFromDisk != null
          && redundancyProvider.allBucketsRecoveredFromDisk.getCount() > 0) {
        return false;
      }
    }

    return true;
  }

  private static class ManageBucketRsp {
    static final ManageBucketRsp NO = new ManageBucketRsp("NO");
    static final ManageBucketRsp YES = new ManageBucketRsp("YES");
    static final ManageBucketRsp NO_INITIALIZING = new ManageBucketRsp("NO_INITIALIZING");
    public static final ManageBucketRsp CLOSED = new ManageBucketRsp("CLOSED");

    private final String name;

    private ManageBucketRsp(String name) {
      this.name = name;
    }

    boolean isRejection() {
      return this == NO || this == NO_INITIALIZING || this == CLOSED;
    }

    boolean isAcceptance() {
      return this == YES;
    }

    boolean isInitializing() {
      return this == NO_INITIALIZING;
    }

    @Override
    public String toString() {
      return "ManageBucketRsp(" + this.name + ")";
    }

    /** return YES if the argument is true, NO if not */
    static ManageBucketRsp valueOf(boolean managed) {
      return managed ? YES : NO;
    }
  }

  private static class BucketMembershipObserverResults {
    final boolean problematicDeparture;
    final InternalDistributedMember primary;

    BucketMembershipObserverResults(boolean re, InternalDistributedMember p) {
      problematicDeparture = re;
      primary = p;
    }

    @Override
    public String toString() {
      return "pDepart:" + problematicDeparture + " primary:" + primary;
    }
  }

  /**
   * Monitors distributed membership for a given bucket
   *
   */
  private class BucketMembershipObserver implements MembershipListener {
    final Bucket bucketToMonitor;
    final AtomicInteger arrivals = new AtomicInteger(0);
    final AtomicBoolean departures = new AtomicBoolean(false);

    public BucketMembershipObserver(Bucket b) {
      this.bucketToMonitor = b;
    }

    public BucketMembershipObserver beginMonitoring() {
      int profilesPresent = this.bucketToMonitor.getBucketAdvisor()
          .addMembershipListenerAndAdviseGeneric(this).size();
      arrivals.addAndGet(profilesPresent);
      return this;
    }

    public void stopMonitoring() {
      this.bucketToMonitor.getBucketAdvisor().removeMembershipListener(this);
    }

    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      if (logger.isDebugEnabled()) {
        logger.debug("Observer for bucket {} member joined {}", this.bucketToMonitor, id);
      }
      synchronized (this) {
        // TODO manipulate failedNodes and verifiedNodeList directly
        arrivals.addAndGet(1);
        notify();
      }
    }

    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {}

    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      if (logger.isDebugEnabled()) {
        logger.debug("Observer for bucket {} member departed {}", this.bucketToMonitor, id);
      }
      synchronized (this) {
        // TODO manipulate failedNodes and verifiedNodeList directly
        departures.getAndSet(true);
        notify();
      }
    }

    /**
     * Wait for expected number of owners to be recognized. When the expected number have been seen,
     * then fetch the primary and report it. If while waiting for the owners to be recognized there
     * is a departure which compromises redundancy
     *
     * @param expectedCount the number of bucket owners to wait for
     * @param expectedOwners the list of owners used when a departure is detected
     * @return if no problematic departures are detected, the primary
     */
    public BucketMembershipObserverResults waitForOwnersGetPrimary(final int expectedCount,
        final Collection<InternalDistributedMember> expectedOwners, String partitionName)
        throws InterruptedException {
      boolean problematicDeparture = false;
      synchronized (this) {
        for (;;) {
          this.bucketToMonitor.getCancelCriterion().checkCancelInProgress(null);

          // If any departures, need to rethink much...
          boolean oldDepartures = departures.get();
          if (oldDepartures) {
            verifyBucketNodes(expectedOwners, partitionName);
            if (expectedOwners.isEmpty()) {
              problematicDeparture = true; // need to pick new victims
            }
            // reselect = true; // need to pick new victims
            arrivals.set(expectedOwners.size());
            departures.set(false);
            if (problematicDeparture) {
              if (logger.isDebugEnabled()) {
                logger.debug("Bucket observer found departed members - retrying");
              }
            }
            break;
          }

          // Look for success...
          int oldArrivals = arrivals.get();
          if (oldArrivals >= expectedCount) {
            // success!
            break;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Waiting for bucket {} to finish being created",
                prRegion.bucketStringForLogs(this.bucketToMonitor.getId()));
          }

          prRegion.checkReadiness();
          final int creationWaitMillis = 5 * 1000;
          wait(creationWaitMillis);

          if (oldArrivals == arrivals.get() && oldDepartures == departures.get()) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.PRHARedundancyProvider_TIME_OUT_WAITING_0_MS_FOR_CREATION_OF_BUCKET_FOR_PARTITIONED_REGION_1_MEMBERS_REQUESTED_TO_CREATE_THE_BUCKET_ARE_2,
                new Object[] {Integer.valueOf(creationWaitMillis), prRegion.getFullPath(),
                    expectedOwners}));
          }
        } // for (;;)
      } // synchronized
      if (problematicDeparture) {
        return new BucketMembershipObserverResults(true, null);
      }
      InternalDistributedMember primmy = bucketToMonitor.getBucketAdvisor().getPrimary();
      if (primmy == null) {
        /*
         * Handle a race where nobody has the bucket. We can't return a null member here because we
         * haven't created the bucket, need to let the higher level code loop.
         */
        return new BucketMembershipObserverResults(true, null);
      } else {
        return new BucketMembershipObserverResults(false, primmy);
      }
    }

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}
  }

  /**
   * This class extends MembershipListener to perform cleanup when a node leaves DistributedSystem.
   *
   */
  protected class PRMembershipListener implements MembershipListener {
    public void memberDeparted(DistributionManager distributionManager,
        final InternalDistributedMember id, final boolean crashed) {
      try {
        DistributedMember dmem = prRegion.getSystem().getDistributedMember();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "MembershipListener invoked on DistributedMember = {} for failed memberId = {}", dmem,
              id);
        }

        if (!prRegion.isCacheClosing() && !prRegion.isDestroyed() && !dmem.equals(id)) {

          Runnable postRecoveryTask = null;

          // Only schedule redundancy recovery if this not a fixed PR.
          if (!PRHARedundancyProvider.this.prRegion.isFixedPartitionedRegion()) {
            postRecoveryTask = new Runnable() {
              public void run() {
                // After the metadata has been cleaned, recover redundancy.
                scheduleRedundancyRecovery(id);
              }
            };
          }
          // Schedule clean up the metadata for the failed member.
          PartitionedRegionHelper.cleanUpMetaDataForRegion(prRegion.getCache(),
              prRegion.getRegionIdentifier(), id, postRecoveryTask);
        }
      } catch (CancelException e) {
        // ignore
      }
    }

    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {}

    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      // no action required
    }

    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}
  }

  /**
   * This class extends MembershipListener to start redundancy recovery when a persistent member is
   * revoked
   *
   */
  protected class PRPersistenceListener extends PersistentStateListener.PersistentStateAdapter {

    // TODO prpersist It seems like this might trigger recovery too often. For example, a rebalance
    // can end up removing a bucket, which would trigger recovery here. We really need to only
    // trigger this thing when a PR region is destroyed. And isn't that code already in there?
    @Override
    public void memberRemoved(PersistentMemberID persistentID, boolean revoked) {
      if (!revoked) {
        return;
      }

      DistributedMember dmem = prRegion.getSystem().getDistributedMember();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Persistent Membership Listener invoked on DistributedMember = {} for removed memberId = {}",
            dmem, persistentID);
      }

      if (!prRegion.isCacheClosing() && !prRegion.isDestroyed()
          && !prRegion.isFixedPartitionedRegion()) {
        scheduleRedundancyRecovery(persistentID);
      }
    }
  }

  public CountDownLatch getAllBucketsRecoveredFromDiskLatch() {
    return allBucketsRecoveredFromDisk;
  }
}
