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

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.internal.cache.ColocationHelper.checkMembersColocation;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.printCollection;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
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
import org.apache.geode.internal.cache.partitioned.DataStoreBuckets;
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
import org.apache.geode.internal.cache.partitioned.PartitionedRegionRebalanceOpFactory;
import org.apache.geode.internal.cache.partitioned.PersistentBucketRecoverer;
import org.apache.geode.internal.cache.partitioned.RecoveryRunnable;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import org.apache.geode.internal.cache.partitioned.rebalance.CompositeDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.FPRDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.RebalanceDirector;
import org.apache.geode.internal.cache.persistence.MembershipFlushRequest;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;

/**
 * This class provides the redundancy management for partitioned region. It will provide the
 * following to the PartitionedRegion:
 *
 * <ol>
 * <li>Redundancy management at the time of bucket creation.
 * <li>Redundancy management at the new node arrival.
 * <li>Redundancy management when the node leaves the partitioned region distributed system
 * gracefully. i.e. Cache.close()
 * <li>Redundancy management at random node failure.
 * </ol>
 */
public class PRHARedundancyProvider {
  private static final Logger logger = LogService.getLogger();

  public static final String TIMEOUT_MSG =
      "If your system has sufficient space, perhaps it is under membership or region creation stress?";

  /**
   * Signature string indicating that not enough stores are available.
   */
  public static final String INSUFFICIENT_STORES_MSG =
      "Advise you to start enough data store nodes";

  private static final boolean DISABLE_CREATE_BUCKET_RANDOMNESS =
      Boolean.getBoolean(GEMFIRE_PREFIX + "DISABLE_CREATE_BUCKET_RANDOMNESS");

  private static final String DATASTORE_DISCOVERY_TIMEOUT_PROPERTY_NAME =
      GEMFIRE_PREFIX + "partitionedRegionDatastoreDiscoveryTimeout";

  /**
   * Signature string indicating that there are enough stores available.
   */
  private static final String SUFFICIENT_STORES_MSG = "Found a member to host a bucket.";

  /**
   * string indicating the attempt to allocate a bucket
   */
  private static final String ALLOCATE_ENOUGH_MEMBERS_TO_HOST_BUCKET =
      "allocate enough members to host a new bucket";

  private static final long INSUFFICIENT_LOGGING_THROTTLE_TIME = TimeUnit.SECONDS.toNanos(
      Integer.getInteger(GEMFIRE_PREFIX + "InsufficientLoggingThrottleTime", 2));

  private static final ThreadLocal<Boolean> forceLocalPrimaries = new ThreadLocal<>();

  @MakeNotStatic
  private static final Long DATASTORE_DISCOVERY_TIMEOUT_MILLISECONDS =
      Long.getLong(DATASTORE_DISCOVERY_TIMEOUT_PROPERTY_NAME);

  @MakeNotStatic
  private static final AtomicLong insufficientLogTimeStamp = new AtomicLong(0);

  @MakeNotStatic
  private final AtomicBoolean firstInsufficientStoresLogged = new AtomicBoolean(false);

  private final PartitionedRegion partitionedRegion;
  private final InternalResourceManager resourceManager;
  private final PartitionedRegionRebalanceOpFactory rebalanceOpFactory;
  private final CompletableFuture<Void> providerStartupTask;

  /**
   * An executor to submit tasks for redundancy recovery too. It makes sure that there will only be
   * one redundancy recovery task in the queue at a time.
   */
  private final OneTaskOnlyExecutor recoveryExecutor;

  private final Object shutdownLock = new Object();

  private final BiFunction<PRHARedundancyProvider, Integer, PersistentBucketRecoverer> persistentBucketRecovererFunction;

  private volatile ScheduledFuture<?> recoveryFuture;

  /**
   * Used to consolidate logging for bucket regions waiting on other members to come online.
   */
  private volatile PersistentBucketRecoverer persistentBucketRecoverer;

  private boolean shutdown;

  /**
   * Constructor for PRHARedundancyProvider.
   *
   * @param partitionedRegion The PartitionedRegion for which the HA redundancy is required to be
   *        managed.
   */
  public PRHARedundancyProvider(PartitionedRegion partitionedRegion,
      InternalResourceManager resourceManager) {
    this(partitionedRegion, resourceManager, PersistentBucketRecoverer::new,
        PartitionedRegionRebalanceOp::new,
        new CompletableFuture<>());
  }

  @VisibleForTesting
  PRHARedundancyProvider(PartitionedRegion partitionedRegion,
      InternalResourceManager resourceManager,
      BiFunction<PRHARedundancyProvider, Integer, PersistentBucketRecoverer> persistentBucketRecovererFunction) {
    this(partitionedRegion, resourceManager, persistentBucketRecovererFunction,
        PartitionedRegionRebalanceOp::new, new CompletableFuture<>());
  }

  @VisibleForTesting
  PRHARedundancyProvider(PartitionedRegion partitionedRegion,
      InternalResourceManager resourceManager,
      BiFunction<PRHARedundancyProvider, Integer, PersistentBucketRecoverer> persistentBucketRecovererFunction,
      PartitionedRegionRebalanceOpFactory rebalanceOpFactory,
      CompletableFuture<Void> providerStartupTask) {
    this.partitionedRegion = partitionedRegion;
    this.resourceManager = resourceManager;
    this.rebalanceOpFactory = rebalanceOpFactory;
    this.providerStartupTask = providerStartupTask;
    recoveryExecutor = new OneTaskOnlyExecutor(resourceManager.getExecutor(),
        () -> InternalResourceManager.getResourceObserver().recoveryConflated(partitionedRegion),
        getThreadMonitorObj());
    this.persistentBucketRecovererFunction = persistentBucketRecovererFunction;
  }

  /**
   * Display bucket allocation status
   *
   * @param partitionedRegion the given region
   * @param allStores the list of available stores. If null, unknown.
   * @param alreadyUsed stores allocated; only used if allStores != null
   * @param forLog true if the generated string is for a log message
   * @return the description string
   */
  private static String regionStatus(PartitionedRegion partitionedRegion,
      Collection<InternalDistributedMember> allStores,
      Collection<InternalDistributedMember> alreadyUsed, boolean forLog) {
    String newLine = forLog ? " " : lineSeparator();
    String spaces = forLog ? "" : "   ";

    StringBuilder sb = new StringBuilder();
    sb.append("Partitioned Region name = ");
    sb.append(partitionedRegion.getFullPath());

    if (allStores != null) {
      sb.append(newLine).append(spaces);
      sb.append("Redundancy level set to ");
      sb.append(partitionedRegion.getRedundantCopies());
      sb.append(newLine);
      sb.append(". Number of available data stores: ");
      sb.append(allStores.size());
      sb.append(newLine).append(spaces);
      sb.append(". Number successfully allocated = ");
      sb.append(alreadyUsed.size());
      sb.append(newLine);
      sb.append(". Data stores: ");
      sb.append(printCollection(allStores));
      sb.append(newLine);
      sb.append(". Data stores successfully allocated: ");
      sb.append(printCollection(alreadyUsed));
      sb.append(newLine);
      sb.append(". Equivalent members: ");
      sb.append(printCollection(partitionedRegion.getDistributionManager().getMembersInThisZone()));
    }

    return sb.toString();
  }

  /**
   * Indicate a timeout due to excessive retries among available peers
   *
   * @param allStores all feasible stores. If null, we don't know.
   * @param alreadyUsed those that have already accepted, only used if allStores != null
   * @param opString description of the operation which timed out
   */
  public static void timedOut(PartitionedRegion partitionedRegion,
      Set<InternalDistributedMember> allStores,
      Collection<InternalDistributedMember> alreadyUsed, String opString, long timeOut) {
    throw new PartitionedRegionStorageException(
        format("Timed out attempting to %s in the partitioned region.%sWaited for: %s ms.",
            opString, regionStatus(partitionedRegion, allStores, alreadyUsed, true), timeOut)
            + TIMEOUT_MSG);
  }

  public PartitionedRegion getPartitionedRegion() {
    return partitionedRegion;
  }

  private Set<InternalDistributedMember> getAllStores(String partitionName) {
    if (partitionName != null) {
      return getFixedPartitionStores(partitionName);
    }
    final Set<InternalDistributedMember> allStores =
        partitionedRegion.getRegionAdvisor().adviseDataStore(true);
    PartitionedRegionDataStore localDataStore = partitionedRegion.getDataStore();
    if (localDataStore != null) {
      allStores.add(partitionedRegion.getDistributionManager().getId());
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
        partitionedRegion.getRegionAdvisor().adviseFixedPartitionDataStores(partitionName);

    List<FixedPartitionAttributesImpl> allFixedPartitionAttributes =
        partitionedRegion.getFixedPartitionAttributesImpl();

    if (allFixedPartitionAttributes != null) {
      for (FixedPartitionAttributesImpl fixedPartitionAttributes : allFixedPartitionAttributes) {
        if (fixedPartitionAttributes.getPartitionName().equals(partitionName)) {
          members.add(partitionedRegion.getMyId());
        }
      }
    }
    return members;
  }

  /**
   * Indicate that we are unable to allocate sufficient stores and the timeout period has passed
   *
   * @param allStores stores we know about
   * @param alreadyUsed ones already committed
   * @param onlyLog true if only a warning log messages should be generated.
   */
  private void insufficientStores(Set<InternalDistributedMember> allStores,
      Collection<InternalDistributedMember> alreadyUsed, boolean onlyLog) {
    String regionStat = regionStatus(partitionedRegion, allStores, alreadyUsed, onlyLog);
    String newLine = onlyLog ? " " : lineSeparator();
    String notEnoughValidNodes = alreadyUsed.isEmpty()
        ? "Unable to find any members to host a bucket in the partitioned region. %s.%s"
        : "Configured redundancy level could not be satisfied. %s to satisfy redundancy for the region.%s";
    if (onlyLog) {
      logger.warn(format(notEnoughValidNodes, INSUFFICIENT_STORES_MSG,
          newLine + regionStat + newLine));
    } else {
      throw new PartitionedRegionStorageException(
          format(notEnoughValidNodes, INSUFFICIENT_STORES_MSG, newLine + regionStat + newLine));
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
  private InternalDistributedMember createBucketInstance(int bucketId, int newBucketSize,
      Collection<InternalDistributedMember> excludedMembers,
      Collection<InternalDistributedMember> alreadyUsed,
      ArrayListWithClearState<InternalDistributedMember> failedMembers, long timeOut,
      Set<InternalDistributedMember> allStores) {

    boolean isDebugEnabled = logger.isDebugEnabled();

    // Recalculate list of candidates
    Set<InternalDistributedMember> candidateMembers = new HashSet<>(allStores);
    candidateMembers.removeAll(alreadyUsed);
    candidateMembers.removeAll(excludedMembers);
    candidateMembers.removeAll(failedMembers);

    if (isDebugEnabled) {
      logger.debug("AllStores={} AlreadyUsed={} excluded={} failed={}", allStores, alreadyUsed,
          excludedMembers, failedMembers);
    }

    if (candidateMembers.isEmpty()) {
      partitionedRegion.checkReadiness();

      // Run out of candidates. Refetch?
      if (System.currentTimeMillis() > timeOut) {
        if (isDebugEnabled) {
          logger.debug("createBucketInstance: ran out of candidates and timed out");
        }
        // fail, let caller signal error
        return null;
      }

      // Recalculate
      candidateMembers = new HashSet<>(allStores);
      candidateMembers.removeAll(alreadyUsed);
      candidateMembers.removeAll(excludedMembers);
      failedMembers.clear();
    }

    if (isDebugEnabled) {
      logger.debug("createBucketInstance: candidateMembers = {}", candidateMembers);
    }

    // If there are no candidates, early out.
    if (candidateMembers.isEmpty()) {
      // no options
      if (isDebugEnabled) {
        logger.debug("createBucketInstance: no valid candidates");
      }
      // failure
      return null;
    }

    // In case of FixedPartitionedRegion, candidateMembers is the set of members on which
    // required fixed partition is defined.
    InternalDistributedMember candidate;
    if (partitionedRegion.isFixedPartitionedRegion()) {
      candidate = candidateMembers.iterator().next();
    } else {
      String colocatedWith =
          partitionedRegion.getAttributes().getPartitionAttributes().getColocatedWith();
      if (colocatedWith != null) {
        candidate = getColocatedDataStore(candidateMembers, alreadyUsed, bucketId, colocatedWith);
      } else {
        Collection<InternalDistributedMember> orderedCandidates = new ArrayList<>(candidateMembers);
        candidate = getPreferredDataStore(orderedCandidates, alreadyUsed);
      }
    }

    if (candidate == null) {
      failedMembers.addAll(candidateMembers);
      return null;
    }

    if (!partitionedRegion.isShadowPR() && !checkMembersColocation(partitionedRegion, candidate)) {
      if (isDebugEnabled) {
        logger.debug(
            "createBucketInstances - Member does not have all of the regions colocated with partitionedRegion {}",
            candidate);
      }
      failedMembers.add(candidate);
      return null;
    }

    if (!candidate.equals(partitionedRegion.getMyId())) {
      PartitionProfile profile =
          partitionedRegion.getRegionAdvisor().getPartitionProfile(candidate);
      if (profile == null) {
        if (isDebugEnabled) {
          logger.debug("createBucketInstance: {}: no partition profile for {}",
              partitionedRegion.getFullPath(), candidate);
        }
        failedMembers.add(candidate);
        return null;
      }
    }

    // Coordinate with any remote close occurring, causing it to wait until
    // this create bucket attempt has been made.
    ManageBucketRsp response =
        createBucketOnMember(bucketId, candidate, newBucketSize, failedMembers.wasCleared());

    // Add targetNode to bucketNodes if successful, else to failedNodeList
    if (response.isAcceptance()) {
      // success!
      return candidate;
    }

    if (isDebugEnabled) {
      logger.debug("createBucketInstance: {}: candidate {} declined to manage bucketId={}: {}",
          partitionedRegion.getFullPath(), candidate,
          partitionedRegion.bucketStringForLogs(bucketId),
          response);
    }
    if (response.equals(ManageBucketRsp.CLOSED)) {
      excludedMembers.add(candidate);
    } else {
      failedMembers.add(candidate);
    }

    return null;
  }

  InternalDistributedMember createBucketOnDataStore(int bucketId, int size,
      RetryTimeKeeper retryTimeKeeper) {
    boolean isDebugEnabled = logger.isDebugEnabled();

    InternalDistributedMember primaryForFixedPartition = null;
    if (partitionedRegion.isFixedPartitionedRegion()) {
      primaryForFixedPartition =
          partitionedRegion.getRegionAdvisor().adviseFixedPrimaryPartitionDataStore(bucketId);
    }

    InternalDistributedMember memberHostingBucket;
    Collection<InternalDistributedMember> attempted = new HashSet<>();
    do {
      partitionedRegion.checkReadiness();
      Set<InternalDistributedMember> available =
          partitionedRegion.getRegionAdvisor().adviseInitializedDataStore();
      available.removeAll(attempted);
      InternalDistributedMember targetMember = null;
      for (InternalDistributedMember member : available) {
        if (available.contains(primaryForFixedPartition)) {
          targetMember = primaryForFixedPartition;
        } else {
          targetMember = member;
        }
        break;
      }
      if (targetMember == null) {
        if (shouldLogInsufficientStores()) {
          insufficientStores(available, Collections.emptySet(), true);
        }
        // this will always throw an exception
        insufficientStores(available, Collections.emptySet(), false);
      }
      try {
        if (isDebugEnabled) {
          logger.debug("Attempting to get data store {} to create the bucket {} for us",
              targetMember,
              partitionedRegion.bucketStringForLogs(bucketId));
        }
        CreateBucketMessage.NodeResponse response =
            CreateBucketMessage.send(targetMember, partitionedRegion, bucketId, size);
        memberHostingBucket = response.waitForResponse();
        if (memberHostingBucket != null) {
          return memberHostingBucket;
        }
      } catch (ForceReattemptException e) {
        // do nothing, we will already check again for a primary.
      }
      attempted.add(targetMember);
    } while ((memberHostingBucket =
        partitionedRegion.getNodeForBucketWrite(bucketId, retryTimeKeeper)) == null);
    return memberHostingBucket;
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
   * picture of bucket distribution in the face of concurrency.
   *
   * <p>
   * This method is now slightly misnamed. Another member could be in the process of creating this
   * same bucket at the same time.
   *
   * @param bucketId Id of the bucket to be created.
   * @param newBucketSize size of the first entry.
   * @return the primary member for the newly created bucket
   * @throws PartitionedRegionStorageException if required # of buckets can not be created to
   *         satisfy redundancy.
   * @throws PartitionedRegionException if d-lock can not be acquired to create bucket.
   * @throws PartitionOfflineException if persistent data recovery is not complete for a partitioned
   *         region referred to in the query.
   */
  public InternalDistributedMember createBucketAtomically(int bucketId, int newBucketSize,
      boolean finishIncompleteCreation, String partitionName)
      throws PartitionedRegionStorageException, PartitionedRegionException,
      PartitionOfflineException {
    boolean isDebugEnabled = logger.isDebugEnabled();

    partitionedRegion.checkPROffline();

    // If there are insufficient stores throw *before* we try acquiring the
    // (very expensive) bucket lock or the (somewhat expensive) monitor on this
    earlySufficientStoresCheck(partitionName);

    synchronized (this) {
      if (partitionedRegion.getCache().isCacheAtShutdownAll()) {
        throw partitionedRegion.getCache().getCacheClosedException("Cache is shutting down");
      }

      if (isDebugEnabled) {
        logger.debug("Starting atomic creation of bucketId={}",
            partitionedRegion.bucketStringForLogs(bucketId));
      }

      long timeOut = System.currentTimeMillis() + computeTimeout();
      BucketMembershipObserver observer = null;
      boolean needToElectPrimary = true;
      InternalDistributedMember bucketPrimary = null;

      try {
        partitionedRegion.checkReadiness();

        Bucket toCreate = partitionedRegion.getRegionAdvisor().getBucket(bucketId);

        if (!finishIncompleteCreation) {
          bucketPrimary = partitionedRegion.getBucketPrimary(bucketId);
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
        ArrayListWithClearState<InternalDistributedMember> failedMembers =
            new ArrayListWithClearState<>();
        Set<InternalDistributedMember> excludedMembers = new HashSet<>();
        Collection<InternalDistributedMember> acceptedMembers = new ArrayList<>();

        for (boolean loggedInsufficientStores = false;;) {
          partitionedRegion.checkReadiness();
          if (partitionedRegion.getCache().isCacheAtShutdownAll()) {
            if (isDebugEnabled) {
              logger.debug("Aborted createBucketAtomically due to ShutdownAll");
            }
            throw partitionedRegion.getCache().getCacheClosedException("Cache is shutting down");
          }

          long timeLeft = timeOut - System.currentTimeMillis();
          if (timeLeft < 0) {
            // It took too long.
            timedOut(partitionedRegion, getAllStores(partitionName), acceptedMembers,
                ALLOCATE_ENOUGH_MEMBERS_TO_HOST_BUCKET, computeTimeout());
          }

          if (isDebugEnabled) {
            logger.debug("createBucketAtomically: have {} ms left to finish this", timeLeft);
          }

          // Always go back to the advisor, see if any fresh data stores are present.
          Set<InternalDistributedMember> allStores = getAllStores(partitionName);

          loggedInsufficientStores = checkSufficientStores(allStores, loggedInsufficientStores);

          InternalDistributedMember candidate = createBucketInstance(bucketId, newBucketSize,
              excludedMembers, acceptedMembers, failedMembers, timeOut, allStores);
          if (candidate != null) {
            if (partitionedRegion.getDistributionManager().enforceUniqueZone()) {
              // enforceUniqueZone property has no effect for a loner
              if (!(partitionedRegion
                  .getDistributionManager() instanceof LonerDistributionManager)) {
                List<InternalDistributedMember> exm = getBuddyMembersInZone(candidate, allStores);
                exm.remove(candidate);
                exm.removeAll(acceptedMembers);
                excludedMembers.addAll(exm);
              } else {
                // log a warning if Loner
                logger.warn(
                    "enforce-unique-host and redundancy-zone properties have no effect for a LonerDistributedSystem.");
              }
            }
          }

          // Get an updated list of bucket owners, which should include
          // buckets created concurrently with this createBucketAtomically call
          acceptedMembers = partitionedRegion.getRegionAdvisor().getBucketOwners(bucketId);

          if (isDebugEnabled) {
            logger.debug("Accepted members: {}", acceptedMembers);
          }

          // set the primary as the candidate in the first iteration if the candidate has accepted
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
          int potentialCandidateCount = allStores.size()
              - (excludedMembers.size() + acceptedMembers.size() + failedMembers.size());

          // Determining exhausted members competes with bucket balancing; it's
          // important to re-visit all failed members since "failed" set may
          // contain datastores which at the moment are imbalanced, but yet could
          // be candidates. If the failed members list is empty, its expected
          // that the next iteration clears the (already empty) list.
          boolean exhaustedPotentialCandidates =
              failedMembers.wasCleared() && potentialCandidateCount <= 0;
          boolean redundancySatisfied =
              acceptedMembers.size() > partitionedRegion.getRedundantCopies();
          boolean bucketNotCreated = acceptedMembers.isEmpty();

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
          if (redundancySatisfied || exhaustedPotentialCandidates) {
            // Tell one of the members to become primary.
            // The rest of the members will be allowed to volunteer for primary.
            endBucketCreation(bucketId, acceptedMembers, bucketPrimary, partitionName);

            int expectedRemoteHosts = acceptedMembers.size()
                - (acceptedMembers.contains(partitionedRegion.getMyId()) ? 1 : 0);

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
              partitionedRegion.getCancelCriterion().checkCancelInProgress(e);
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }

            needToElectPrimary = false;

            return bucketPrimary;
          }
        }
      } catch (CancelException | RegionDestroyedException e) {
        // We don't need to elect a primary if the cache was closed. The other members will
        // take care of it. This ensures we don't compromise redundancy.
        needToElectPrimary = false;
        throw e;
      } catch (PartitionOfflineException e) {
        throw e;
      } catch (RuntimeException e) {
        if (isDebugEnabled) {
          logger.debug("Unable to create new bucket {}: {}", bucketId, e.getMessage(), e);
        }

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
            endBucketCreation(bucketId,
                partitionedRegion.getRegionAdvisor().getBucketOwners(bucketId),
                bucketPrimary, partitionName);
          } catch (Exception e) {
            // if region is going down, then no warning level logs
            if (e instanceof CancelException
                || partitionedRegion.getCancelCriterion().isCancelInProgress()) {
              logger.debug("Exception trying choose a primary after bucket creation failure", e);
            } else {
              logger.warn("Exception trying choose a primary after bucket creation failure", e);
            }
          }
        }
      }
    }
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
    acceptedMembers = new HashSet<>(acceptedMembers);

    // This is for FPR, for a given bucket id , make sure that for given bucket
    // id , only the datastore on which primary partition is defined for this
    // bucket becomes the primary. If primary partition is not available then
    // secondary partition will become primary
    if (partitionName != null) {
      if (isLocalPrimary(partitionName)) {
        targetPrimary = partitionedRegion.getMyId();
      } else {
        targetPrimary =
            partitionedRegion.getRegionAdvisor().adviseFixedPrimaryPartitionDataStore(bucketId);
        if (targetPrimary == null) {
          Set<InternalDistributedMember> fpDataStores = getFixedPartitionStores(partitionName);
          targetPrimary = fpDataStores.iterator().next();
        }
      }
    }

    if (targetPrimary == null) {
      // we need to select the same primary as chosen earlier (e.g.
      // the parent's in case of colocation) so it is now passed
      targetPrimary =
          getPreferredDataStore(acceptedMembers, Collections.emptySet());
    }

    boolean isHosting = acceptedMembers.remove(partitionedRegion.getDistributionManager().getId());

    EndBucketCreationMessage.send(acceptedMembers, targetPrimary, partitionedRegion, bucketId);

    if (isHosting) {
      endBucketCreationLocally(bucketId, targetPrimary);
    }
  }

  private boolean isLocalPrimary(String partitionName) {
    List<FixedPartitionAttributesImpl> allFixedPartitionAttributes =
        partitionedRegion.getFixedPartitionAttributesImpl();
    if (allFixedPartitionAttributes != null) {
      for (FixedPartitionAttributesImpl fixedPartitionAttributes : allFixedPartitionAttributes) {
        if (fixedPartitionAttributes.getPartitionName().equals(partitionName)
            && fixedPartitionAttributes.isPrimary()) {
          return true;
        }
      }
    }
    return false;
  }

  public void endBucketCreationLocally(int bucketId, InternalDistributedMember newPrimary) {
    // Don't elect ourselves as primary or tell others to persist our ID
    // if this member has been destroyed.
    if (partitionedRegion.getCancelCriterion().isCancelInProgress()
        || partitionedRegion.isDestroyed()) {
      return;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("endBucketCreationLocally: for region {} bucketId={} new primary: {}",
          partitionedRegion.getFullPath(), bucketId, newPrimary);
    }

    BucketAdvisor bucketAdvisor = partitionedRegion.getRegionAdvisor().getBucketAdvisor(bucketId);
    ProxyBucketRegion proxyBucketRegion = bucketAdvisor.getProxyBucketRegion();
    BucketPersistenceAdvisor persistentAdvisor = proxyBucketRegion.getPersistenceAdvisor();

    // prevent multiple threads from ending bucket creation at the same time.
    synchronized (proxyBucketRegion) {
      if (persistentAdvisor != null) {
        BucketRegion realBucket = proxyBucketRegion.getCreatedBucketRegion();
        if (realBucket != null) {
          PersistentMemberID persistentId = realBucket.getPersistentID();
          persistentAdvisor.endBucketCreation(persistentId);
        }
      }

      // We've received an endBucketCreationMessage, but the primary
      // may not have. So now we wait for the chosen member to become primary.
      bucketAdvisor.setPrimaryElector(newPrimary);

      if (partitionedRegion.getGemFireCache().getMyId().equals(newPrimary)) {
        // If we're the chosen primary, volunteer for primary now
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

        // If the bucket has had a primary, that means the chosen bucket was primary for a while.
        // Go ahead and clear the primary elector field.
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

    List<PartitionedRegion> colocatedWithList = ColocationHelper.getColocatedChildRegions(
        partitionedRegion);
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
   */
  private List<InternalDistributedMember> getBuddyMembersInZone(
      InternalDistributedMember acceptedMember, Collection<InternalDistributedMember> allStores) {
    DistributionManager dm = partitionedRegion.getDistributionManager();
    List<InternalDistributedMember> buddies = dm.getMembersInSameZone(acceptedMember);
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
    Set<InternalDistributedMember> currentStores = getAllStores(partitionName);
    if (currentStores.isEmpty()) {
      if (shouldLogInsufficientStores()) {
        insufficientStores(currentStores, emptyList(), true);
      }
      insufficientStores(currentStores, emptyList(), false);
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
    if (firstInsufficientStoresLogged.compareAndSet(false, true)
        || delta >= INSUFFICIENT_LOGGING_THROTTLE_TIME) {
      insufficientLogTimeStamp.set(now);
      return true;
    }
    return false;
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
      long millis = DATASTORE_DISCOVERY_TIMEOUT_MILLISECONDS;
      // only positive values allowed
      if (millis > 0) {
        return millis;
      }
    }
    return partitionedRegion.getRetryTimeout();
  }

  /**
   * Check to determine that there are enough datastore VMs to start the bucket creation processes.
   * Log a warning or throw an exception indicating when there are not enough datastore VMs.
   *
   * @param allStores All known data store instances (including local)
   * @param loggedInsufficientStores indicates whether a warning has been logged
   * @return true when a warning has been logged, false if a warning should be logged.
   */
  private boolean checkSufficientStores(Set<InternalDistributedMember> allStores,
      boolean loggedInsufficientStores) {
    // Report (only once) if insufficient data store have been detected.
    if (!loggedInsufficientStores) {
      if (allStores.isEmpty()) {
        insufficientStores(allStores, emptyList(), true);
        return true;
      }
    } else {
      if (!allStores.isEmpty()) {
        // Excellent, sufficient resources were found!
        logger.info("{} Region name, {}", SUFFICIENT_STORES_MSG, partitionedRegion.getFullPath());
        return false;
      }
      // Already logged warning, there are no datastores
      insufficientStores(allStores, emptyList(), false);
    }
    return loggedInsufficientStores;
  }

  /**
   * Clean up locally created bucket and tell other VMs to attempt recovering redundancy
   *
   * @param bucketId the bucket identifier
   */
  private void cleanUpBucket(int bucketId) {
    Set<InternalDistributedMember> dataStores =
        partitionedRegion.getRegionAdvisor().adviseDataStore();
    BucketBackupMessage.send(dataStores, partitionedRegion, bucketId);
  }

  public void finishIncompleteBucketCreation(int bucketId) {
    String partitionName = null;
    if (partitionedRegion.isFixedPartitionedRegion()) {
      FixedPartitionAttributesImpl fpa =
          PartitionedRegionHelper.getFixedPartitionAttributesForBucket(partitionedRegion, bucketId);
      partitionName = fpa.getPartitionName();
    }
    createBucketAtomically(bucketId, 0, true, partitionName);
  }

  /**
   * Creates bucket with ID bucketId on targetNode. This method will also create the bucket for all
   * of the child colocated PRs.
   *
   * @param isRebalance true if bucket creation is directed by rebalancing
   * @return true if the bucket was sucessfully created
   */
  public boolean createBackupBucketOnMember(int bucketId, InternalDistributedMember targetMember,
      boolean isRebalance, boolean replaceOfflineData, InternalDistributedMember fromMember,
      boolean forceCreation) {
    if (logger.isDebugEnabled()) {
      logger.debug("createBackupBucketOnMember for bucketId={} member: {}",
          partitionedRegion.bucketStringForLogs(bucketId), targetMember);
    }

    if (!targetMember.equals(partitionedRegion.getMyId())) {
      PartitionProfile profile =
          partitionedRegion.getRegionAdvisor().getPartitionProfile(targetMember);
      if (profile == null) {
        return false;
      }

      try {
        ManageBackupBucketMessage.NodeResponse response =
            ManageBackupBucketMessage.send(targetMember, partitionedRegion, bucketId, isRebalance,
                replaceOfflineData, fromMember, forceCreation);

        if (response.waitForAcceptance()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "createBackupBucketOnMember: Bucket creation succeed for bucketId={} on member = {}",
                partitionedRegion.bucketStringForLogs(bucketId), targetMember);
          }

          return true;
        }

        if (logger.isDebugEnabled()) {
          logger.debug(
              "createBackupBucketOnMember: Bucket creation failed for bucketId={} on member = {}",
              partitionedRegion.bucketStringForLogs(bucketId), targetMember);
        }

        return false;

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
          // no log needed
        } else if (e instanceof CancelException
            || e.getCause() != null && e.getCause() instanceof CancelException) {
          // no need to log exceptions caused by cache closure
        } else {
          logger.warn("Exception creating partition on {}", targetMember, e);
        }
        return false;
      }
    }

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    boolean bucketManaged =
        dataStore != null && dataStore.grabBucket(bucketId, fromMember, forceCreation,
            replaceOfflineData, isRebalance, null, false).equals(CreateBucketResult.CREATED);
    if (!bucketManaged) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "createBackupBucketOnMember: Local data store refused to accommodate the data for bucketId={} dataStore={}",
            partitionedRegion.bucketStringForLogs(bucketId), dataStore);
      }
    }
    return bucketManaged;
  }

  private boolean getForceLocalPrimaries() {
    boolean result = false;
    Boolean forceLocalPrimariesValue = forceLocalPrimaries.get();
    if (forceLocalPrimariesValue != null) {
      result = forceLocalPrimariesValue;
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
  private ManageBucketRsp createBucketOnMember(int bucketId, InternalDistributedMember targetMember,
      int newBucketSize, boolean forceCreation) {
    if (logger.isDebugEnabled()) {
      logger.debug("createBucketOnMember for bucketId={} member: {}{}",
          partitionedRegion.bucketStringForLogs(bucketId), targetMember,
          forceCreation ? " forced" : "");
    }

    if (!targetMember.equals(partitionedRegion.getMyId())) {
      PartitionProfile profile =
          partitionedRegion.getRegionAdvisor().getPartitionProfile(targetMember);
      if (profile == null) {
        return ManageBucketRsp.NO;
      }

      try {
        NodeResponse response = ManageBucketMessage.send(targetMember, partitionedRegion, bucketId,
            newBucketSize, forceCreation);

        if (response.waitForAcceptance()) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "createBucketOnMember: Bucket creation succeed for bucketId={} on member = {}",
                partitionedRegion.bucketStringForLogs(bucketId), targetMember);
          }

          return ManageBucketRsp.YES;
        }

        if (logger.isDebugEnabled()) {
          logger.debug(
              "createBucketOnMember: Bucket creation failed for bucketId={} on member = {}",
              partitionedRegion.bucketStringForLogs(bucketId), targetMember);
        }

        return response.rejectedDueToInitialization() ? ManageBucketRsp.NO_INITIALIZING
            : ManageBucketRsp.NO;

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
            || e.getCause() != null && e.getCause() instanceof CancelException) {
          // no need to log exceptions caused by cache closure
          return ManageBucketRsp.CLOSED;
        }
        if (e instanceof ForceReattemptException) {
          // no log needed
        } else {
          logger.warn("Exception creating partition on {}", targetMember, e);
        }
        return ManageBucketRsp.NO;
      }
    }

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    boolean bucketManaged = dataStore != null && dataStore.handleManageBucketRequest(bucketId,
        newBucketSize, partitionedRegion.getMyId(), forceCreation);
    if (!bucketManaged) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "createBucketOnMember: Local data store not able to accommodate the data for bucketId={}",
            partitionedRegion.bucketStringForLogs(bucketId));
      }
    }
    return ManageBucketRsp.valueOf(bucketManaged);
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
    Assert.assertTrue(prName != null);
    PartitionedRegion colocatedRegion = ColocationHelper.getColocatedRegion(partitionedRegion);
    Region<?, ?> prRoot = PartitionedRegionHelper.getPRRoot(partitionedRegion.getCache());
    PartitionRegionConfig config =
        (PartitionRegionConfig) prRoot.get(partitionedRegion.getRegionIdentifier());

    if (!config.isColocationComplete()) {
      throw new IllegalStateException(
          "Cannot create buckets, as colocated regions are not configured to be at the same nodes.");
    }

    RegionAdvisor advisor = colocatedRegion.getRegionAdvisor();
    if (alreadyUsed.isEmpty()) {
      InternalDistributedMember primary = advisor.getPrimaryMemberForBucket(bucketId);
      if (!candidates.contains(primary)) {
        return null;
      }
      return primary;
    }

    Set<InternalDistributedMember> bucketOwnersSet = advisor.getBucketOwners(bucketId);
    bucketOwnersSet.retainAll(candidates);
    Collection<InternalDistributedMember> members = new ArrayList<>(bucketOwnersSet);
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
   * @param candidates collection of InternalDistributedMember, potential datastores
   * @param alreadyUsed data stores already in use
   * @return a member with the fewest buckets or null if no datastores
   */
  private InternalDistributedMember getPreferredDataStore(
      Collection<InternalDistributedMember> candidates,
      Collection<InternalDistributedMember> alreadyUsed) {
    // has a primary already been chosen?
    boolean forPrimary = alreadyUsed.isEmpty();

    if (forPrimary && getForceLocalPrimaries()) {
      PartitionedRegionDataStore localDataStore = partitionedRegion.getDataStore();
      if (localDataStore != null) {
        return partitionedRegion.getMyId();
      }
    }

    if (candidates.size() == 1) {
      return candidates.iterator().next();
    }
    Assert.assertTrue(candidates.size() > 1);

    // Convert peers to DataStoreBuckets
    List<DataStoreBuckets> stores = partitionedRegion.getRegionAdvisor()
        .adviseFilteredDataStores(new HashSet<>(candidates));

    DistributionManager dm = partitionedRegion.getDistributionManager();

    // Add local member as a candidate, if appropriate
    InternalDistributedMember localMember = dm.getId();
    PartitionedRegionDataStore localDataStore = partitionedRegion.getDataStore();
    if (localDataStore != null && candidates.contains(localMember)) {
      int bucketCount = localDataStore.getBucketsManaged();
      int priCount = localDataStore.getNumberOfPrimaryBucketsManaged();
      int localMaxMemory = partitionedRegion.getLocalMaxMemory();
      stores.add(new DataStoreBuckets(localMember, bucketCount, priCount, localMaxMemory));
    }

    if (stores.isEmpty()) {
      return null;
    }

    // ---------------------------------------------
    // Calculate all hosts who already have this bucket
    Collection<InternalDistributedMember> existingHosts = new HashSet<>();
    for (InternalDistributedMember mem : alreadyUsed) {
      existingHosts.addAll(dm.getMembersInSameZone(mem));
    }

    Comparator<DataStoreBuckets> comparator = (d1, d2) -> {
      boolean host1Used = existingHosts.contains(d1.memberId());
      boolean host2Used = existingHosts.contains(d2.memberId());

      if (!host1Used && host2Used) {
        // host1 preferred
        return -1;
      }
      if (host1Used && !host2Used) {
        // host2 preferred
        return 1;
      }

      // Look for least loaded
      float load1;
      float load2;
      if (forPrimary) {
        load1 = d1.numPrimaries() / (float) d1.localMaxMemoryMB();
        load2 = d2.numPrimaries() / (float) d2.localMaxMemoryMB();
      } else {
        load1 = d1.numBuckets() / (float) d1.localMaxMemoryMB();
        load2 = d2.numBuckets() / (float) d2.localMaxMemoryMB();
      }

      int result = Float.compare(load1, load2);
      if (result == 0) {
        // if they have the same load, choose the member with the higher localMaxMemory
        result = d2.localMaxMemoryMB() - d1.localMaxMemoryMB();
      }
      return result;
    };

    // ---------------------------------------------
    // First step is to sort datastores first by those whose hosts don't
    // hold this bucket, and then secondarily by loading.
    stores.sort(comparator);

    if (logger.isDebugEnabled()) {
      logger.debug(fancyFormatBucketAllocation("Sorted ", stores, existingHosts));
    }

    // ---------------------------------------------
    // Always add the first datastore and note just how good it is.
    DataStoreBuckets bestDataStore = stores.get(0);
    List<DataStoreBuckets> bestStores = new ArrayList<>();
    bestStores.add(bestDataStore);

    boolean allStoresInUse = alreadyUsed.contains(bestDataStore.memberId());

    // ---------------------------------------------
    // Collect all of the other hosts in this sorted list that are as good as the very first one.
    for (int i = 1; i < stores.size(); i++) {
      DataStoreBuckets aDataStore = stores.get(i);
      if (!allStoresInUse && alreadyUsed.contains(aDataStore.memberId())) {
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

    return bestStores.get(chosen).memberId();
  }

  /**
   * Adds a membership listener to watch for member departures, and schedules a task to recover
   * redundancy of existing buckets
   */
  void startRedundancyRecovery() {
    partitionedRegion.getRegionAdvisor().addMembershipListener(new PRMembershipListener());
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
   * <p>
   * After the member name, the +/- indicates whether or not this bucket is already hosted on the
   * given member. This is followed by the number of hosted primaries followed by the number of
   * hosted non-primary buckets.
   *
   * @param prefix first part of message to print
   * @param dataStores list of stores
   * @param existingStores to mark those already in use
   */
  private String fancyFormatBucketAllocation(String prefix, Iterable<DataStoreBuckets> dataStores,
      Collection<InternalDistributedMember> existingStores) {
    StringBuilder logStr = new StringBuilder();
    if (prefix != null) {
      logStr.append(prefix);
    }
    logStr.append("Bucket Allocation for prId=").append(partitionedRegion.getPRId()).append(":");
    logStr.append(lineSeparator());

    for (Object dataStore : dataStores) {
      DataStoreBuckets buckets = (DataStoreBuckets) dataStore;
      logStr.append(buckets.memberId()).append(": ");
      if (existingStores.contains(buckets.memberId())) {
        logStr.append("+");
      } else {
        logStr.append("-");
      }
      logStr.append(buckets.numPrimaries());
      logStr.append("/");
      logStr.append(buckets.numBuckets() - buckets.numPrimaries());

      logStr.append(lineSeparator());
    }
    return logStr.toString();
  }

  /**
   * Verifies the members and removes the members that are either not present in the
   * DistributedSystem or are no longer part of the PartitionedRegion (close/localDestroy has been
   * performed.) .
   *
   * @param members collection of members to scan and modify
   */
  private void verifyBucketNodes(Collection<InternalDistributedMember> members,
      String partitionName) {
    if (members == null || members.isEmpty()) {
      return;
    }

    // Revisit region advisor, get current bucket stores.
    Set<InternalDistributedMember> availableMembers = getAllStores(partitionName);

    for (Iterator<InternalDistributedMember> iterator = members.iterator(); iterator.hasNext();) {
      InternalDistributedMember node = iterator.next();
      if (!availableMembers.contains(node)) {
        if (logger.isDebugEnabled()) {
          logger.debug("verifyBucketNodes: removing member {}", node);
        }
        iterator.remove();
        Assert.assertTrue(!members.contains(node), "return value does not contain " + node);
      }
    }
  }

  /**
   * Schedule a task to perform redundancy recovery for a new node or for the node departed.
   */
  private void scheduleRedundancyRecovery(Object failedMemberId) {
    boolean isStartup = failedMemberId == null;
    long delay;
    boolean movePrimaries;

    if (isStartup) {
      delay = partitionedRegion.getPartitionAttributes().getStartupRecoveryDelay();
      movePrimaries = !Boolean
          .getBoolean(GEMFIRE_PREFIX + "DISABLE_MOVE_PRIMARIES_ON_STARTUP");
    } else {
      delay = partitionedRegion.getPartitionAttributes().getRecoveryDelay();
      movePrimaries = false;
    }

    final boolean requiresRedundancyRecovery = delay >= 0;

    if (!requiresRedundancyRecovery) {
      return;
    }
    if (!partitionedRegion.isDataStore()) {
      return;
    }

    Runnable task = new RecoveryRunnable(this) {
      @Override
      public void run2() {
        try {
          boolean isFixedPartitionedRegion = partitionedRegion.isFixedPartitionedRegion();

          // always replace offline data for fixed partitioned regions -
          // this guarantees we create the buckets we are supposed to create on this node.
          boolean replaceOfflineData = isFixedPartitionedRegion || !isStartup;

          RebalanceDirector director;
          if (isFixedPartitionedRegion) {
            director = new FPRDirector(true, movePrimaries);
          } else {
            director = new CompositeDirector(true, true, false, movePrimaries);
          }

          PartitionedRegionRebalanceOp rebalance = rebalanceOpFactory.create(
              partitionedRegion, false, director, replaceOfflineData, false);

          long start = partitionedRegion.getPrStats().startRecovery();

          if (isFixedPartitionedRegion) {
            rebalance.executeFPA();
          } else {
            rebalance.execute();
          }

          partitionedRegion.getPrStats().endRecovery(start);
          recoveryFuture = null;
          providerStartupTask.complete(null);
        } catch (CancelException e) {
          logger.debug("Cache closed while recovery in progress");
          providerStartupTask.completeExceptionally(e);
        } catch (RegionDestroyedException e) {
          logger.debug("Region destroyed while recovery in progress");
          providerStartupTask.completeExceptionally(e);
        } catch (Exception e) {
          logger.error("Unexpected exception during bucket recovery", e);
          providerStartupTask.completeExceptionally(e);
        }
      }
    };

    synchronized (shutdownLock) {
      if (!shutdown) {
        try {
          if (logger.isDebugEnabled()) {
            if (isStartup) {
              logger.debug("{} scheduling redundancy recovery in {} ms", partitionedRegion, delay);
            } else {
              logger.debug(
                  "partitionedRegion scheduling redundancy recovery after departure/crash/error in {} in {} ms",
                  failedMemberId, delay);
            }
          }
          recoveryFuture = recoveryExecutor.schedule(task, delay, MILLISECONDS);
          resourceManager.addStartupTask(providerStartupTask);
        } catch (RejectedExecutionException e) {
          // ok, the executor is shutting down.
        }
      }
    }
  }

  public boolean isRedundancyImpaired() {
    int numBuckets = partitionedRegion.getPartitionAttributes().getTotalNumBuckets();
    int targetRedundancy = partitionedRegion.getPartitionAttributes().getRedundantCopies();

    for (int i = 0; i < numBuckets; i++) {
      int redundancy = partitionedRegion.getRegionAdvisor().getBucketRedundancy(i);
      if (redundancy < targetRedundancy && redundancy != -1 || redundancy > targetRedundancy) {
        return true;
      }
    }

    return false;
  }

  boolean recoverPersistentBuckets() {
    /*
     * To handle a case where ParallelGatewaySender is persistent but userPR is not First recover
     * the GatewaySender buckets for ParallelGatewaySender irrespective of whether colocation is
     * complete or not.
     */
    PartitionedRegion leaderRegion = ColocationHelper.getLeaderRegion(partitionedRegion);


    // Check if the leader region or some child shadow PR region is persistent
    // and return the first persistent region found
    PartitionedRegion persistentLeader = getPersistentLeader();

    // If there is no persistent region in the colocation chain, no need to recover.
    if (persistentLeader == null) {
      return true;
    }

    if (!checkMembersColocation(leaderRegion, leaderRegion.getDistributionManager().getId())) {
      if (logger.isDebugEnabled()) {
        logger.debug("Skipping persistent recovery of {} because colocation is not complete for {}",
            partitionedRegion, leaderRegion);
      }
      return false;
    }

    ProxyBucketRegion[] proxyBucketArray =
        persistentLeader.getRegionAdvisor().getProxyBucketArray();
    if (proxyBucketArray.length == 0) {
      throw new IllegalStateException("Unexpected empty proxy bucket array");
    }
    for (ProxyBucketRegion proxyBucket : proxyBucketArray) {
      proxyBucket.initializePersistenceAdvisor();
    }

    Set<InternalDistributedMember> peers = partitionedRegion.getRegionAdvisor().adviseGeneric();

    // We need to make sure here that we don't run into this race condition:
    // 1) We get a membership view from member A
    // 2) Member B removes itself, and distributes to us and A. We don't remove B
    // 3) We apply the membership view from A, which includes B.
    // That will add B back into the set.
    // This state flush will make sure that any membership changes
    // That are in progress on the peers are finished.
    MembershipFlushRequest.send(peers, partitionedRegion.getDistributionManager(),
        partitionedRegion.getFullPath());

    List<ProxyBucketRegion> bucketsNotHostedLocally = new ArrayList<>(proxyBucketArray.length);
    List<ProxyBucketRegion> bucketsHostedLocally = new ArrayList<>(proxyBucketArray.length);

    createPersistentBucketRecoverer(proxyBucketArray.length);

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
    for (ProxyBucketRegion proxyBucket : proxyBucketArray) {
      if (proxyBucket.getPersistenceAdvisor().wasHosting()) {
        RecoveryRunnable recoveryRunnable = new RecoveryRunnable(this) {

          @Override
          public void run() {
            // make sure that we always count down this latch, even if the region was destroyed.
            try {
              super.run();
            } finally {
              if (getPersistentBucketRecoverer() != null) {
                getPersistentBucketRecoverer().countDown();
              }
            }
          }

          @Override
          public void run2() {
            proxyBucket.recoverFromDiskRecursively();
          }
        };

        Thread recoveryThread =
            new LoggingThread("Recovery thread for bucket " + proxyBucket.getName(),
                false, recoveryRunnable);
        recoveryThread.start();
        bucketsHostedLocally.add(proxyBucket);
      } else {
        bucketsNotHostedLocally.add(proxyBucket);
      }
    }

    try {
      // try to recover the local buckets before the proxy buckets. This will allow us to detect
      // any ConflictingDataException before the proxy buckets update their membership view.
      for (ProxyBucketRegion proxyBucket : bucketsHostedLocally) {
        proxyBucket.waitForPrimaryPersistentRecovery();
      }
      for (ProxyBucketRegion proxyBucket : bucketsNotHostedLocally) {
        proxyBucket.recoverFromDiskRecursively();
      }
    } finally {
      if (getPersistentBucketRecoverer() != null) {
        getPersistentBucketRecoverer().countDown(bucketsNotHostedLocally.size());
      }
    }

    return true;
  }

  @VisibleForTesting
  void createPersistentBucketRecoverer(int proxyBuckets) {
    persistentBucketRecoverer = persistentBucketRecovererFunction.apply(this, proxyBuckets);
    persistentBucketRecoverer.startLoggingThread();
  }

  @VisibleForTesting
  PersistentBucketRecoverer getPersistentBucketRecoverer() {
    return persistentBucketRecoverer;
  }

  /**
   * Check to see if any colocated region of the current region is persistent. It's not enough to
   * check just the leader region, because a child region might be a persistent parallel WAN queue,
   * which is allowed.
   *
   * @return the most senior region in the colocation chain (closest to the leader) that is
   *         persistent.
   */
  private PartitionedRegion getPersistentLeader() {
    PartitionedRegion leader = ColocationHelper.getLeaderRegion(partitionedRegion);
    return findPersistentRegionRecursively(leader);
  }

  private PartitionedRegion findPersistentRegionRecursively(PartitionedRegion partitionedRegion) {
    if (partitionedRegion.getDataPolicy().withPersistence()) {
      return partitionedRegion;
    }
    for (PartitionedRegion child : ColocationHelper.getColocatedChildRegions(partitionedRegion)) {
      PartitionedRegion leader = findPersistentRegionRecursively(child);
      if (leader != null) {
        return leader;
      }
    }
    return null;
  }

  void scheduleCreateMissingBuckets() {
    if (partitionedRegion.getColocatedWith() != null
        && ColocationHelper.isColocationComplete(partitionedRegion)) {
      Runnable task = new CreateMissingBucketsTask(this);
      final InternalResourceManager resourceManager =
          partitionedRegion.getGemFireCache().getInternalResourceManager();
      resourceManager.getExecutor().execute(task);
    }
  }

  public void shutdown() {
    synchronized (shutdownLock) {
      shutdown = true;
      ScheduledFuture<?> recoveryFuture = this.recoveryFuture;
      if (recoveryFuture != null) {
        recoveryFuture.cancel(false);
        recoveryExecutor.purge();
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
  public InternalPRInfo buildPartitionedRegionInfo(boolean internal, LoadProbe loadProbe) {
    PartitionedRegion pr = partitionedRegion;

    if (pr == null) {
      return null;
    }

    int configuredBucketCount = pr.getTotalNumberOfBuckets();
    int createdBucketCount = pr.getRegionAdvisor().getCreatedBucketsCount();

    int lowRedundancyBucketCount = pr.getRedundancyTracker().getLowRedundancyBuckets();
    int configuredRedundantCopies = pr.getRedundantCopies();
    int actualRedundantCopies = pr.getRedundancyTracker().getActualRedundancy();

    PartitionedRegionDataStore dataStore = pr.getDataStore();

    Set<InternalDistributedMember> datastores = pr.getRegionAdvisor().adviseDataStore();

    Set<InternalPartitionDetails> memberDetails = new TreeSet<>();

    OfflineMemberDetails offlineMembers = null;
    boolean fetchOfflineMembers = false;
    if (dataStore != null) {
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

    return new PartitionRegionInfoImpl(pr.getFullPath(), configuredBucketCount,
        createdBucketCount, lowRedundancyBucketCount, configuredRedundantCopies,
        actualRedundantCopies, memberDetails, colocatedWithPath, offlineMembers);
  }

  /**
   * Retrieve the set of members which are currently offline for all buckets.
   */
  public OfflineMemberDetailsImpl fetchOfflineMembers() {
    ProxyBucketRegion[] proxyBuckets = partitionedRegion.getRegionAdvisor().getProxyBucketArray();
    Set<PersistentMemberID>[] offlineMembers = new Set[proxyBuckets.length];
    for (int i = 0; i < proxyBuckets.length; i++) {
      ProxyBucketRegion proxyBucket = proxyBuckets[i];
      if (partitionedRegion.getDataPolicy().withPersistence()) {
        Set<PersistentMemberID> persistedMembers =
            proxyBucket.getPersistenceAdvisor().getMissingMembers();
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
  public InternalPartitionDetails buildPartitionMemberDetails(boolean internal,
      LoadProbe loadProbe) {
    final PartitionedRegion pr = partitionedRegion;

    PartitionedRegionDataStore dataStore = pr.getDataStore();
    if (dataStore == null) {
      return null;
    }

    long size = 0;
    InternalDistributedMember localMember = pr.getMyId();

    int configuredBucketCount = pr.getTotalNumberOfBuckets();
    long[] bucketSizes = new long[configuredBucketCount];

    // key: bid, value: size
    Map<Integer, Integer> bucketSizeMap = dataStore.getSizeLocally();

    for (Map.Entry<Integer, Integer> me : bucketSizeMap.entrySet()) {
      int bid = me.getKey();
      long bucketSize = dataStore.getBucketSize(bid);
      bucketSizes[bid] = bucketSize;
      size += bucketSize;
    }

    InternalPartitionDetails localDetails;
    if (internal) {
      waitForPersistentBucketRecoveryOrClose();

      PRLoad prLoad = loadProbe.getLoad(pr);
      localDetails =
          new PartitionMemberInfoImpl(localMember, pr.getLocalMaxMemory() * 1024L * 1024L, size,
              dataStore.getBucketsManaged(), dataStore.getNumberOfPrimaryBucketsManaged(), prLoad,
              bucketSizes);
    } else {
      localDetails =
          new PartitionMemberInfoImpl(localMember, pr.getLocalMaxMemory() * 1024L * 1024L, size,
              dataStore.getBucketsManaged(), dataStore.getNumberOfPrimaryBucketsManaged());
    }
    return localDetails;
  }

  /**
   * Wait for all persistent buckets to be recovered from disk, or for the region to be closed,
   * whichever happens first.
   */
  private void waitForPersistentBucketRecoveryOrClose() {
    if (getPersistentBucketRecoverer() != null) {
      getPersistentBucketRecoverer().await(
          PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION, MILLISECONDS);
    }

    List<PartitionedRegion> colocatedRegions =
        ColocationHelper.getColocatedChildRegions(partitionedRegion);
    for (PartitionedRegion child : colocatedRegions) {
      child.getRedundancyProvider().waitForPersistentBucketRecoveryOrClose();
    }
  }

  /**
   * Wait for all persistent buckets to be recovered from disk, regardless of whether the region is
   * currently being closed.
   */
  void waitForPersistentBucketRecovery() {
    if (getPersistentBucketRecoverer() != null) {
      getPersistentBucketRecoverer().await();
    }
  }

  boolean isPersistentRecoveryComplete() {
    if (!checkMembersColocation(partitionedRegion, partitionedRegion.getMyId())) {
      return false;
    }

    if (getPersistentBucketRecoverer() != null
        && !getPersistentBucketRecoverer().hasRecoveryCompleted()) {
      return false;
    }

    Map<String, PartitionedRegion> colocatedRegions =
        ColocationHelper.getAllColocationRegions(partitionedRegion);

    for (PartitionedRegion region : colocatedRegions.values()) {
      PRHARedundancyProvider redundancyProvider = region.getRedundancyProvider();
      if (redundancyProvider.getPersistentBucketRecoverer() != null &&
          !redundancyProvider.getPersistentBucketRecoverer().hasRecoveryCompleted()) {
        return false;
      }
    }
    return true;
  }

  private ThreadsMonitoring getThreadMonitorObj() {
    DistributionManager distributionManager = partitionedRegion.getDistributionManager();
    if (distributionManager != null) {
      return distributionManager.getThreadMonitoring();
    }
    return null;
  }

  /**
   * Monitors distributed membership for a given bucket
   */
  private class BucketMembershipObserver implements MembershipListener {

    private final Bucket bucketToMonitor;
    private final AtomicInteger arrivals = new AtomicInteger(0);
    private final AtomicBoolean departures = new AtomicBoolean(false);

    private BucketMembershipObserver(Bucket bucketToMonitor) {
      this.bucketToMonitor = bucketToMonitor;
    }

    private BucketMembershipObserver beginMonitoring() {
      int profilesPresent = bucketToMonitor.getBucketAdvisor()
          .addMembershipListenerAndAdviseGeneric(this).size();
      arrivals.addAndGet(profilesPresent);
      return this;
    }

    private void stopMonitoring() {
      bucketToMonitor.getBucketAdvisor().removeMembershipListener(this);
    }

    @Override
    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      if (logger.isDebugEnabled()) {
        logger.debug("Observer for bucket {} member joined {}", bucketToMonitor, id);
      }
      synchronized (this) {
        arrivals.addAndGet(1);
        notifyAll();
      }
    }

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      if (logger.isDebugEnabled()) {
        logger.debug("Observer for bucket {} member departed {}", bucketToMonitor, id);
      }
      synchronized (this) {
        departures.getAndSet(true);
        notifyAll();
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
    private BucketMembershipObserverResults waitForOwnersGetPrimary(int expectedCount,
        Collection<InternalDistributedMember> expectedOwners, String partitionName)
        throws InterruptedException {
      boolean problematicDeparture = false;
      synchronized (this) {
        while (true) {
          bucketToMonitor.getCancelCriterion().checkCancelInProgress(null);

          // If any departures, need to rethink much...
          boolean oldDepartures = departures.get();
          if (oldDepartures) {
            verifyBucketNodes(expectedOwners, partitionName);
            if (expectedOwners.isEmpty()) {
              problematicDeparture = true; // need to pick new victims
            }

            // need to pick new victims
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
                partitionedRegion.bucketStringForLogs(bucketToMonitor.getId()));
          }

          partitionedRegion.checkReadiness();

          int creationWaitMillis = 5 * 1000;
          wait(creationWaitMillis);

          if (oldArrivals == arrivals.get() && oldDepartures == departures.get()) {
            logger.warn(
                "Time out waiting {} ms for creation of bucket for partitioned region {}. Members requested to create the bucket are: {}",
                creationWaitMillis, partitionedRegion.getFullPath(), expectedOwners);
          }
        }
      }

      if (problematicDeparture) {
        return new BucketMembershipObserverResults(true, null);
      }

      InternalDistributedMember primary = bucketToMonitor.getBucketAdvisor().getPrimary();
      if (primary == null) {
        /*
         * Handle a race where nobody has the bucket. We can't return a null member here because we
         * haven't created the bucket, need to let the higher level code loop.
         */
        return new BucketMembershipObserverResults(true, null);
      }
      return new BucketMembershipObserverResults(false, primary);
    }
  }

  /**
   * This class extends MembershipListener to perform cleanup when a node leaves DistributedSystem.
   */
  private class PRMembershipListener implements MembershipListener {

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      try {
        DistributedMember member = partitionedRegion.getSystem().getDistributedMember();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "MembershipListener invoked on DistributedMember = {} for failed memberId = {}",
              member,
              id);
        }

        if (!partitionedRegion.isCacheClosing() && !partitionedRegion.isDestroyed()
            && !member.equals(id)) {
          Runnable postRecoveryTask = null;

          // Only schedule redundancy recovery if this not a fixed PR.
          if (!partitionedRegion.isFixedPartitionedRegion()) {
            postRecoveryTask = () -> {
              // After the metadata has been cleaned, recover redundancy.
              scheduleRedundancyRecovery(id);
            };
          }
          // Schedule clean up the metadata for the failed member.
          PartitionedRegionHelper.cleanUpMetaDataForRegion(partitionedRegion.getCache(),
              partitionedRegion.getRegionIdentifier(), id, postRecoveryTask);
        }
      } catch (CancelException ignore) {
        // ignore
      }
    }
  }

  private static class ManageBucketRsp {

    @Immutable
    private static final ManageBucketRsp NO = new ManageBucketRsp("NO");
    @Immutable
    private static final ManageBucketRsp YES = new ManageBucketRsp("YES");
    @Immutable
    private static final ManageBucketRsp NO_INITIALIZING = new ManageBucketRsp("NO_INITIALIZING");
    @Immutable
    private static final ManageBucketRsp CLOSED = new ManageBucketRsp("CLOSED");

    private final String name;

    private ManageBucketRsp(String name) {
      this.name = name;
    }

    private boolean isAcceptance() {
      return this == YES;
    }

    @Override
    public String toString() {
      return "ManageBucketRsp(" + name + ")";
    }

    /** return YES if the argument is true, NO if not */
    private static ManageBucketRsp valueOf(boolean managed) {
      return managed ? YES : NO;
    }
  }

  private static class BucketMembershipObserverResults {
    private final boolean problematicDeparture;
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

  private static class ArrayListWithClearState<T> extends ArrayList<T> {

    private static final long serialVersionUID = 1L;
    private volatile boolean wasCleared;

    private boolean wasCleared() {
      return wasCleared;
    }

    @Override
    public void clear() {
      super.clear();
      wasCleared = true;
    }
  }
}
