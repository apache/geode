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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.ColocationHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegion.RecoveryLock;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.PartitionRebalanceDetailsImpl;
import org.apache.geode.internal.cache.control.ResourceManagerStats;
import org.apache.geode.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import org.apache.geode.internal.cache.partitioned.MoveBucketMessage.MoveBucketResponse;
import org.apache.geode.internal.cache.partitioned.RemoveBucketMessage.RemoveBucketResponse;
import org.apache.geode.internal.cache.partitioned.rebalance.BucketOperator;
import org.apache.geode.internal.cache.partitioned.rebalance.BucketOperatorImpl;
import org.apache.geode.internal.cache.partitioned.rebalance.BucketOperatorWrapper;
import org.apache.geode.internal.cache.partitioned.rebalance.ParallelBucketOperator;
import org.apache.geode.internal.cache.partitioned.rebalance.RebalanceDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.SimulatedBucketOperator;
import org.apache.geode.internal.cache.partitioned.rebalance.model.AddressComparor;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;
import org.apache.geode.internal.logging.LogService;

/**
 * This class performs a rebalance on a single partitioned region.
 *
 * There are three main classes involved in the rebalance - this class, the
 * PartitionedRegionLoadModel, and the RebalanceDirector. This class owns the overall rebalance
 * process, and takes care of gathering the data from all members, preventing concurrent rebalances,
 * and forwarding operations for the rebalance to the appropriate members.
 *
 * The PartitionedRegionLoadModel model of the system that is constructed by this class. It contains
 * information about what buckets are where and how big they are. It has methods to find low
 * redundancy buckets or determine where the best place to move a bucket is.
 *
 * The RebalanceDirector is responsible for actually deciding what to do at each step of a
 * rebalance. There are several different director implementations for different types of
 * rebalancing. The most common on is the CompositeDirector, which first satisfies redundancy, moves
 * buckets, and then moves primaries.
 *
 * There is also a FPRDirector that creates buckets and moves primaries for fixed partititioned
 * regions.
 */
@SuppressWarnings("synthetic-access")
public class PartitionedRegionRebalanceOp {
  private static final Logger logger = LogService.getLogger();

  private static final int MAX_PARALLEL_OPERATIONS =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_PARALLEL_BUCKET_RECOVERIES", 8);
  private final boolean DEBUG =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "LOG_REBALANCE");

  private final boolean simulate;
  private final boolean replaceOfflineData;
  private final PartitionedRegion leaderRegion;
  private final PartitionedRegion targetRegion;
  private Collection<PartitionedRegion> colocatedRegions;
  private final AtomicBoolean cancelled;
  private final ResourceManagerStats stats;
  private final boolean isRebalance; // true indicates a rebalance instead of recovery

  private volatile boolean membershipChange = false;

  private final RebalanceDirector director;

  /**
   * Create a rebalance operation for a single region.
   *
   * @param region the region to rebalance
   * @param simulate true to only simulate rebalancing, without actually doing anything
   * @param replaceOfflineData true to replace offline copies of buckets with new live copies of
   *        buckets
   * @param isRebalance true if this op is a full rebalance instead of a more limited redundancy
   *        recovery
   */
  public PartitionedRegionRebalanceOp(PartitionedRegion region, boolean simulate,
      RebalanceDirector director, boolean replaceOfflineData, boolean isRebalance) {
    this(region, simulate, director, replaceOfflineData, isRebalance, new AtomicBoolean(), null);
  }

  /**
   * Create a rebalance operation for a single region.
   *
   * @param region the region to rebalance
   * @param simulate true to only simulate rebalancing, without actually doing anything
   * @param replaceOfflineData true to replace offline copies of buckets with new live copies of
   *        buckets
   * @param isRebalance true if this op is a full rebalance instead of a more limited redundancy
   *        recovery
   * @param cancelled the AtomicBoolean reference used for cancellation; if any code sets the AB
   *        value to true then the rebalance will be cancelled
   * @param stats the ResourceManagerStats to use for rebalancing stats
   */
  public PartitionedRegionRebalanceOp(PartitionedRegion region, boolean simulate,
      RebalanceDirector director, boolean replaceOfflineData, boolean isRebalance,
      AtomicBoolean cancelled, ResourceManagerStats stats) {

    PartitionedRegion leader = ColocationHelper.getLeaderRegion(region);
    Assert.assertTrue(leader != null);

    // set the region we are rebalancing to be leader of the colocation group.
    this.leaderRegion = leader;
    this.targetRegion = region;
    this.simulate = simulate;
    this.director = director;
    this.cancelled = cancelled;
    this.replaceOfflineData = replaceOfflineData;
    this.isRebalance = isRebalance;
    this.stats = simulate ? null : stats;
  }

  /**
   * Do the actual rebalance
   *
   * @return the details of the rebalance.
   */
  public Set<PartitionRebalanceInfo> execute() {
    long start = System.nanoTime();
    InternalResourceManager resourceManager =
        InternalResourceManager.getInternalResourceManager(leaderRegion.getCache());
    MembershipListener listener = new MembershipChangeListener();
    if (isRebalance) {
      InternalResourceManager.getResourceObserver().rebalancingStarted(targetRegion);
    } else {
      InternalResourceManager.getResourceObserver().recoveryStarted(targetRegion);
    }
    RecoveryLock lock = null;
    try {
      if (!checkAndSetColocatedRegions()) {
        return Collections.emptySet();
      }

      // Early out if this was an automatically triggered rebalance and we now
      // have full redundancy.
      if (!isRebalanceNecessary()) {
        return Collections.emptySet();
      }

      if (!simulate) {
        lock = leaderRegion.getRecoveryLock();
        lock.lock();
      }

      // Check this again after getting the lock, because someone might
      // have fixed it already.
      if (!isRebalanceNecessary()) {
        return Collections.emptySet();
      }

      // register a listener to notify us if the new members leave or join.
      // When a membership change occurs, we want to restart the rebalancing
      // from the beginning.
      // TODO rebalance - we should really add a membership listener to ALL of
      // the colocated regions.
      leaderRegion.getRegionAdvisor().addMembershipListener(listener);
      PartitionedRegionLoadModel model = null;

      InternalCache cache = leaderRegion.getCache();
      Map<PartitionedRegion, InternalPRInfo> detailsMap = fetchDetails(cache);
      BucketOperatorWrapper serialOperator = getBucketOperator(detailsMap);
      ParallelBucketOperator parallelOperator = new ParallelBucketOperator(MAX_PARALLEL_OPERATIONS,
          cache.getDistributionManager().getExecutors().getWaitingThreadPool(), serialOperator);
      model = buildModel(parallelOperator, detailsMap, resourceManager);
      for (PartitionRebalanceDetailsImpl details : serialOperator.getDetailSet()) {
        details.setPartitionMemberDetailsBefore(
            model.getPartitionedMemberDetails(details.getRegionPath()));
      }

      director.initialize(model);

      for (;;) {
        if (cancelled.get()) {
          return Collections.emptySet();
        }
        if (membershipChange) {
          membershipChange = false;
          // refetch the partitioned region details after
          // a membership change.
          debug("Rebalancing {} detected membership changes. Refetching details", leaderRegion);
          if (this.stats != null) {
            this.stats.incRebalanceMembershipChanges(1);
          }
          model.waitForOperations();
          detailsMap = fetchDetails(cache);
          model = buildModel(parallelOperator, detailsMap, resourceManager);
          director.membershipChanged(model);
        }

        leaderRegion.checkClosed();
        cache.getCancelCriterion().checkCancelInProgress(null);

        if (logger.isDebugEnabled()) {
          logger.debug("Rebalancing {} Model:{}\n", leaderRegion, model);
        }

        if (!director.nextStep()) {
          // Stop when the director says we can't rebalance any more.
          break;
        }
      }

      debug("Rebalancing {} complete. Model:{}\n", leaderRegion, model);
      long end = System.nanoTime();

      for (PartitionRebalanceDetailsImpl details : serialOperator.getDetailSet()) {
        if (!simulate) {
          details.setTime(end - start);
        }
        details.setPartitionMemberDetailsAfter(
            model.getPartitionedMemberDetails(details.getRegionPath()));
      }

      return Collections.<PartitionRebalanceInfo>unmodifiableSet(serialOperator.getDetailSet());
    } finally {
      if (lock != null) {
        try {
          lock.unlock();
        } catch (CancelException e) {
          // lock service has been destroyed
        } catch (Exception e) {
          logger.error("Unable to release recovery lock",
              e);
        }
      }
      try {
        if (isRebalance) {
          InternalResourceManager.getResourceObserver().rebalancingFinished(targetRegion);
        } else {
          InternalResourceManager.getResourceObserver().recoveryFinished(targetRegion);
        }
      } catch (Exception e) {
        logger.error("Error in resource observer", e);
      }

      try {
        leaderRegion.getRegionAdvisor().removeMembershipListener(listener);
      } catch (Exception e) {
        logger.error("Error in resource observer", e);
      }
    }
  }

  /**
   * Set the list of colocated regions, and check to make sure that colocation is complete.
   *
   * @return true if colocation is complete.
   */
  protected boolean checkAndSetColocatedRegions() {
    // Early out of this VM doesn't have all of the regions that are
    // supposed to be colocated with this one.
    // TODO rebalance - there is a race here between this check and the
    // earlier acquisition of the list
    // of colocated regions. Really we should get a list of all colocated
    // regions on all nodes.
    if (!ColocationHelper.checkMembersColocation(leaderRegion,
        leaderRegion.getDistributionManager().getDistributionManagerId())) {
      return false;
    }

    // find all of the regions which are colocated with this region
    // TODO rebalance - this should really be all of the regions which are colocated
    // anywhere I think. We need to make sure we don't do a rebalance unless we have
    // all of the colocated regions others do.
    Map<String, PartitionedRegion> colocatedRegionsMap =
        ColocationHelper.getAllColocationRegions(targetRegion);
    colocatedRegionsMap.put(targetRegion.getFullPath(), targetRegion);
    final LinkedList<PartitionedRegion> colocatedRegions = new LinkedList<PartitionedRegion>();
    for (PartitionedRegion colocatedRegion : colocatedRegionsMap.values()) {

      // Fix for 49340 - make sure all colocated regions are initialized, so
      // that we know the region has recovered from disk
      if (!colocatedRegion.isInitialized()) {
        return false;
      }
      if (colocatedRegion.getColocatedWith() == null) {
        colocatedRegions.addFirst(colocatedRegion);
      } else {
        colocatedRegions.addLast(colocatedRegion);
      }
    }
    this.colocatedRegions = colocatedRegions;

    return true;
  }

  /**
   * For FPR we will creatd buckets and make primaries as specified by FixedPartitionAttributes. We
   * have to just create buckets and make primaries for the local node.
   *
   * @return the details of the rebalance.
   */
  public Set<PartitionRebalanceInfo> executeFPA() {
    if (logger.isDebugEnabled()) {
      logger.debug("Rebalancing buckets for fixed partitioned region {}", this.targetRegion);
    }

    long start = System.nanoTime();
    InternalCache cache = leaderRegion.getCache();
    InternalResourceManager resourceManager =
        InternalResourceManager.getInternalResourceManager(cache);
    InternalResourceManager.getResourceObserver().recoveryStarted(targetRegion);
    try {
      if (!checkAndSetColocatedRegions()) {
        return Collections.emptySet();
      }
      // If I am a datastore of a FixedPartition, I will be hosting bucket so no
      // need of redundancy check.
      // No need to attach listener as well, we know that we are just creating bucket
      // for primary and secondary. We are not creating extra bucket for any of peers
      // who goes down.

      PartitionedRegionLoadModel model = null;
      Map<PartitionedRegion, InternalPRInfo> detailsMap = fetchDetails(cache);
      BucketOperatorWrapper operator = getBucketOperator(detailsMap);

      model = buildModel(operator, detailsMap, resourceManager);
      for (PartitionRebalanceDetailsImpl details : operator.getDetailSet()) {
        details.setPartitionMemberDetailsBefore(
            model.getPartitionedMemberDetails(details.getRegionPath()));
      }

      /*
       * We haen't taken the distributed recovery lock as only this node is creating bucket for
       * itself. It will take bucket creation lock anyway. To move primary too, it has to see what
       * all bucket it can host as primary and make them. We don't need to do all the calculation
       * for fair balance between the nodes as this is a fixed partitioned region.
       */

      if (logger.isDebugEnabled()) {
        logger.debug("Rebalancing FPR {} Model:{}\n", leaderRegion, model);
      }

      director.initialize(model);
      // This will perform all of the required operations.
      director.nextStep();

      if (logger.isDebugEnabled()) {
        logger.debug("Rebalancing FPR {} complete. Model:{}\n", leaderRegion, model);
      }
      long end = System.nanoTime();

      for (PartitionRebalanceDetailsImpl details : operator.getDetailSet()) {
        if (!simulate) {
          details.setTime(end - start);
        }
        details.setPartitionMemberDetailsAfter(
            model.getPartitionedMemberDetails(details.getRegionPath()));
      }

      return Collections.<PartitionRebalanceInfo>unmodifiableSet(operator.getDetailSet());
    } finally {
      try {
        InternalResourceManager.getResourceObserver().recoveryFinished(targetRegion);
      } catch (Exception e) {
        logger.debug("Error in resource observer", e);
      }
    }
  }

  private Map<PartitionedRegion, InternalPRInfo> fetchDetails(InternalCache cache) {
    LoadProbe probe = cache.getInternalResourceManager().getLoadProbe();
    Map<PartitionedRegion, InternalPRInfo> detailsMap =
        new LinkedHashMap<PartitionedRegion, InternalPRInfo>(colocatedRegions.size());
    for (PartitionedRegion colocatedRegion : colocatedRegions) {
      if (ColocationHelper.isColocationComplete(colocatedRegion)) {
        InternalPRInfo info =
            colocatedRegion.getRedundancyProvider().buildPartitionedRegionInfo(true, probe);
        detailsMap.put(colocatedRegion, info);
      }
    }
    return detailsMap;
  }

  private BucketOperatorWrapper getBucketOperator(
      Map<PartitionedRegion, InternalPRInfo> detailsMap) {
    Set<PartitionRebalanceDetailsImpl> rebalanceDetails =
        new HashSet<PartitionRebalanceDetailsImpl>(detailsMap.size());
    for (Map.Entry<PartitionedRegion, InternalPRInfo> entry : detailsMap.entrySet()) {
      rebalanceDetails.add(new PartitionRebalanceDetailsImpl(entry.getKey()));
    }
    BucketOperator operator =
        simulate ? new SimulatedBucketOperator() : new BucketOperatorImpl(this);
    BucketOperatorWrapper wrapper =
        new BucketOperatorWrapper(operator, rebalanceDetails, stats, leaderRegion);
    return wrapper;
  }

  /**
   * Build a model of the load on the partitioned region, which can determine which buckets to move,
   * etc.
   *
   */
  private PartitionedRegionLoadModel buildModel(BucketOperator operator,
      Map<PartitionedRegion, InternalPRInfo> detailsMap, InternalResourceManager resourceManager) {
    PartitionedRegionLoadModel model;

    final boolean isDebugEnabled = logger.isDebugEnabled();

    final DistributionManager dm = leaderRegion.getDistributionManager();
    AddressComparor comparor = new AddressComparor() {

      @Override
      public boolean areSameZone(InternalDistributedMember member1,
          InternalDistributedMember member2) {
        return dm.areInSameZone(member1, member2);
      }

      @Override
      public boolean enforceUniqueZones() {
        return dm.enforceUniqueZone();
      }
    };


    int redundantCopies = leaderRegion.getRedundantCopies();
    int totalNumberOfBuckets = leaderRegion.getTotalNumberOfBuckets();
    Set<InternalDistributedMember> criticalMembers =
        resourceManager.getResourceAdvisor().adviseCriticalMembers();;
    boolean removeOverRedundancy = true;

    debug("Building Model for rebalancing " + leaderRegion + ". redundantCopies=" + redundantCopies
        + ", totalNumBuckets=" + totalNumberOfBuckets + ", criticalMembers=" + criticalMembers
        + ", simulate=" + simulate);


    model = new PartitionedRegionLoadModel(operator, redundantCopies, totalNumberOfBuckets,
        comparor, criticalMembers, leaderRegion);

    for (Map.Entry<PartitionedRegion, InternalPRInfo> entry : detailsMap.entrySet()) {
      PartitionedRegion region = entry.getKey();
      InternalPRInfo details = entry.getValue();

      OfflineMemberDetails offlineDetails;
      if (replaceOfflineData) {
        offlineDetails = OfflineMemberDetails.EMPTY_DETAILS;
      } else {
        offlineDetails = details.getOfflineMembers();
      }
      boolean enforceLocalMaxMemory = !region.isEntryEvictionPossible();

      debug("Added Region to model region=" + region + ", offlineDetails=" + offlineDetails
          + ", enforceLocalMaxMemory=" + enforceLocalMaxMemory);

      for (PartitionMemberInfo memberDetails : details.getPartitionMemberInfo()) {
        debug("For Region: " + region + ", Member: " + memberDetails.getDistributedMember()
            + "LOAD=" + ((InternalPartitionDetails) memberDetails).getPRLoad()
            + ", equivalentMembers=" + dm.getMembersInSameZone(
                (InternalDistributedMember) memberDetails.getDistributedMember()));
      }
      Set<InternalPartitionDetails> memberDetailSet = details.getInternalPartitionDetails();

      model.addRegion(region.getFullPath(), memberDetailSet, offlineDetails, enforceLocalMaxMemory);
    }

    model.initialize();

    debug("Rebalancing {} starting. Model:\n{}", leaderRegion, model);

    return model;
  }

  private void debug(String message, Object... params) {
    if (logger.isDebugEnabled()) {
      logger.debug(message, params);
    } else if (logger.isInfoEnabled() && DEBUG) {
      logger.info(message, params);
    }

  }

  /**
   * Create a redundant bucket on the target member
   *
   * @param target the member on which to create the redundant bucket
   * @param bucketId the identifier of the bucket
   * @return true if the redundant bucket was created
   */
  public boolean createRedundantBucketForRegion(InternalDistributedMember target, int bucketId) {
    return getLeaderRegion().getRedundancyProvider().createBackupBucketOnMember(bucketId, target,
        isRebalance, replaceOfflineData, null, true);
  }

  /**
   * Remove a redundant bucket on the target member
   *
   * @param target the member on which to create the redundant bucket
   * @param bucketId the identifier of the bucket
   * @return true if the redundant bucket was removed
   */
  public boolean removeRedundantBucketForRegion(InternalDistributedMember target, int bucketId) {
    boolean removed = false;
    if (getLeaderRegion().getDistributionManager().getId().equals(target)) {
      // invoke directly on local member...
      removed = getLeaderRegion().getDataStore().removeBucket(bucketId, false);
    } else {
      // send message to remote member...
      RemoveBucketResponse response =
          RemoveBucketMessage.send(target, getLeaderRegion(), bucketId, false);
      if (response != null) {
        removed = response.waitForResponse();
      }
    }
    return removed;
  }

  /**
   * Move the primary of a bucket to the target member
   *
   * @param target the member which should be primary
   * @param bucketId the identifier of the bucket
   * @return true if the move was successful
   */
  public boolean movePrimaryBucketForRegion(InternalDistributedMember target, int bucketId) {
    boolean movedPrimary = false;
    if (getLeaderRegion().getDistributionManager().getId().equals(target)) {
      // invoke directly on local member...
      BucketAdvisor bucketAdvisor = getLeaderRegion().getRegionAdvisor().getBucketAdvisor(bucketId);
      if (bucketAdvisor.isHosting()) {
        movedPrimary = bucketAdvisor.becomePrimary(isRebalance);
      }
    } else {
      // send message to remote member...
      BecomePrimaryBucketResponse response =
          BecomePrimaryBucketMessage.send(target, getLeaderRegion(), bucketId, isRebalance);
      if (response != null) {
        movedPrimary = response.waitForResponse();
      }
    }
    return movedPrimary;
  }

  /**
   * Move a bucket from the provided source to the target
   *
   * @param source member that contains the bucket
   * @param target member which should receive the bucket
   * @param bucketId the identifier of the bucket
   * @return true if the bucket was moved
   */
  public boolean moveBucketForRegion(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId) {
    boolean movedBucket = false;
    if (getLeaderRegion().getDistributionManager().getId().equals(target)) {
      // invoke directly on local member...
      movedBucket = getLeaderRegion().getDataStore().moveBucket(bucketId, source, false);
    } else {
      // send message to remote member...
      MoveBucketResponse response =
          MoveBucketMessage.send(target, getLeaderRegion(), bucketId, source);
      if (response != null) {
        movedBucket = response.waitForResponse();
      }
    }
    return movedBucket;
  }

  private boolean isRebalanceNecessary() {
    // Fixed partitions will always be rebalanced.
    // Persistent partitions that have recovered from disk will
    // also need to rebalance primaries
    return isRebalance || director.isRebalanceNecessary(
        leaderRegion.getRedundancyProvider().isRedundancyImpaired(),
        leaderRegion.getDataPolicy().withPersistence());
  }

  public PartitionedRegion getLeaderRegion() {
    return leaderRegion;
  }

  private class MembershipChangeListener implements MembershipListener {

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "PartitionedRegionRebalanceOP - membership changed, restarting rebalancing for region {}",
            targetRegion);
      }
      membershipChange = true;
    }

    @Override
    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "PartitionedRegionRebalanceOP - membership changed, restarting rebalancing for region {}",
            targetRegion);
      }
      membershipChange = true;
    }

    @Override
    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
      // do nothing.
    }

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}
  }
}
