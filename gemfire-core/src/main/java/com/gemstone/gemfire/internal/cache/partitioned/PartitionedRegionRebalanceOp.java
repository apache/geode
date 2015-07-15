/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned;

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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRebalanceInfo;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.BucketAdvisor;
import com.gemstone.gemfire.internal.cache.ColocationHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion.RecoveryLock;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.PartitionRebalanceDetailsImpl;
import com.gemstone.gemfire.internal.cache.control.ResourceManagerStats;
import com.gemstone.gemfire.internal.cache.partitioned.BecomePrimaryBucketMessage.BecomePrimaryBucketResponse;
import com.gemstone.gemfire.internal.cache.partitioned.MoveBucketMessage.MoveBucketResponse;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveBucketMessage.RemoveBucketResponse;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.BucketOperator;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.ParallelBucketOperator;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.AddressComparor;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.RebalanceDirector;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.SimulatedBucketOperator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * This class performs a rebalance on a single partitioned region. 
 * 
 * There are three main classes involved in the rebalance - this class,
 * the PartitionedRegionLoadModel, and the RebalanceDirector. This class
 * owns the overall rebalance process, and takes care of gathering the data
 * from all members, preventing concurrent rebalances, and forwarding operations
 * for the rebalance to the appropriate members.
 * 
 * The PartitionedRegionLoadModel model of the system that is constructed by this
 * class. It contains information about what buckets are where and how big they
 * are. It has methods to find low redundancy buckets or determine where the best
 * place to move a bucket is.
 * 
 * The RebalanceDirector is responsible for actually deciding what to do at
 * each step of a rebalance. There are several different director implementations
 * for different types of rebalancing. The most common on is the CompositeDirector,
 * which first satisfies redundancy, moves buckets, and then moves primaries.
 * 
 * There is also a FPRDirector that creates buckets and moves primaries for
 * fixed partititioned regions.
 * 
 * @author dsmith
 *
 */
@SuppressWarnings("synthetic-access")
public class PartitionedRegionRebalanceOp {
  private static final Logger logger = LogService.getLogger();
  
  private static final int MAX_PARALLEL_OPERATIONS = Integer.getInteger("gemfire.MAX_PARALLEL_BUCKET_RECOVERIES", 8);
  
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
   * @param region the region to rebalance
   * @param simulate true to only simulate rebalancing, without actually doing anything
   * @param replaceOfflineData true to replace offline copies of buckets with new live copies of buckets
   * @param isRebalance true if this op is a full rebalance instead of a more
   * limited redundancy recovery
   */
  public PartitionedRegionRebalanceOp(PartitionedRegion region, boolean simulate, RebalanceDirector director, boolean replaceOfflineData, boolean isRebalance) {
    this(region, simulate, director, replaceOfflineData,
        isRebalance, new AtomicBoolean(), null);
  }
  
  /**
   * Create a rebalance operation for a single region.
   * @param region the region to rebalance
   * @param simulate true to only simulate rebalancing, without actually doing anything
   * @param replaceOfflineData true to replace offline copies of buckets with new live copies of buckets
   * @param isRebalance true if this op is a full rebalance instead of a more
   * limited redundancy recovery
   * @param cancelled the AtomicBoolean reference used for cancellation; if
   * any code sets the AB value to true then the rebalance will be cancelled
   * @param stats the ResourceManagerStats to use for rebalancing stats
   */
  public PartitionedRegionRebalanceOp(PartitionedRegion region,
      boolean simulate, RebalanceDirector director, boolean replaceOfflineData, boolean isRebalance,
      AtomicBoolean cancelled, ResourceManagerStats stats) {
    
    PartitionedRegion leader = ColocationHelper.getLeaderRegion(region);
    Assert.assertTrue(leader != null);
    
    //set the region we are rebalancing to be leader of the colocation group.
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
   * @return the details of the rebalance.
   */
  public Set<PartitionRebalanceInfo> execute() {
    long start = System.nanoTime();
    InternalResourceManager resourceManager = InternalResourceManager.getInternalResourceManager(leaderRegion.getCache());
    MembershipListener listener = new MembershipChangeListener();
    if(isRebalance) {
      InternalResourceManager.getResourceObserver().rebalancingStarted(targetRegion);
    } else {
      InternalResourceManager.getResourceObserver().recoveryStarted(targetRegion);
    }
    RecoveryLock lock = null;
    try {
      if(!checkAndSetColocatedRegions()) {
        return Collections.emptySet();
      }
      
      //Early out if this was an automatically triggered rebalance and we now 
      //have full redundancy.
      if (!isRebalanceNecessary()) { 
        return Collections.emptySet();
      }
      
      if (!simulate) {
        lock = leaderRegion.getRecoveryLock();
        lock.lock();
      }
      
      //Check this again after getting the lock, because someone might
      //have fixed it already.
      if (!isRebalanceNecessary()) {
        return Collections.emptySet();
      }
      
      //register a listener to notify us if the new members leave or join.
      //When a membership change occurs, we want to restart the rebalancing
      //from the beginning.
      //TODO rebalance - we should really add a membership listener to ALL of 
      //the colocated regions. 
      leaderRegion.getRegionAdvisor().addMembershipListener(listener);
      PartitionedRegionLoadModel model = null;


      GemFireCacheImpl cache = (GemFireCacheImpl) leaderRegion.getCache();
      Map<PartitionedRegion, InternalPRInfo> detailsMap = fetchDetails(cache);
      BucketOperatorWrapper serialOperator = getBucketOperator(detailsMap);
      ParallelBucketOperator parallelOperator = new ParallelBucketOperator(MAX_PARALLEL_OPERATIONS, cache.getDistributionManager().getWaitingThreadPool(), serialOperator);
      model = buildModel(parallelOperator, detailsMap, resourceManager);
      for(PartitionRebalanceDetailsImpl details : serialOperator.getDetailSet()) {
        details.setPartitionMemberDetailsBefore(model.getPartitionedMemberDetails(details.getRegionPath()));
      }

      director.initialize(model);
      
      for(;;) {
        if(cancelled.get()) {
          return Collections.emptySet();
        }
        if(membershipChange) {
          membershipChange = false;
          //refetch the partitioned region details after
          //a membership change.
          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} detected membership changes. Refetching details", leaderRegion);
          }
          if(this.stats != null) {
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
          //Stop when the director says we can't rebalance any more.
          break;
        }
      }
      
      if (logger.isDebugEnabled()) {
        logger.debug("Rebalancing {} complete. Model:{}\n", leaderRegion, model);
      }
      long end = System.nanoTime();
      
      for(PartitionRebalanceDetailsImpl details : serialOperator.getDetailSet()) {
        if(!simulate) {
          details.setTime(end - start);
        }
        details.setPartitionMemberDetailsAfter(model.getPartitionedMemberDetails(details.getRegionPath()));
      }

      return Collections.<PartitionRebalanceInfo>unmodifiableSet(serialOperator.getDetailSet());
    } finally {
      if(lock != null) {
        try {
          lock.unlock();
        } catch (CancelException e) {
          // lock service has been destroyed
        } catch(Exception e) {
         logger.error(LocalizedMessage.create(LocalizedStrings.PartitionedRegionRebalanceOp_UNABLE_TO_RELEASE_RECOVERY_LOCK), e);
        }
      }
      try {
        if(isRebalance) {
          InternalResourceManager.getResourceObserver().rebalancingFinished(targetRegion);
        } else {
          InternalResourceManager.getResourceObserver().recoveryFinished(targetRegion);
        }
      } catch(Exception e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PartitionedRegionRebalanceOp_ERROR_IN_RESOURCE_OBSERVER), e);
      }
      
      try {
        leaderRegion.getRegionAdvisor().removeMembershipListener(listener);
      } catch(Exception e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PartitionedRegionRebalanceOp_ERROR_IN_RESOURCE_OBSERVER), e);
      }
    }
  }

  /**
   * Set the list of colocated regions, and check to make sure that colocation 
   * is complete.
   * @return true if colocation is complete.
   */
  protected boolean checkAndSetColocatedRegions() {
    //Early out of this VM doesn't have all of  the regions that are 
    //supposed to be colocated with this one.
    //TODO rebalance - there is a race here between this check and the 
    //earlier acquisition of the list
    //of colocated regions. Really we should get a list of all colocated 
    //regions on all nodes.
    if(!ColocationHelper.checkMembersColocation(leaderRegion, 
        leaderRegion.getDistributionManager().getDistributionManagerId())) {
      return false;
    }
    
  //find all of the regions which are colocated with this region
    //TODO rebalance - this should really be all of the regions which are colocated
    //anywhere I think. We need to make sure we don't do a rebalance unless we have
    //all of the colocated regions others do.
    Map<String, PartitionedRegion> colocatedRegionsMap = 
        ColocationHelper.getAllColocationRegions(targetRegion);
    colocatedRegionsMap.put(targetRegion.getFullPath(), targetRegion);
    final LinkedList<PartitionedRegion> colocatedRegions =
      new LinkedList<PartitionedRegion>();
    for (PartitionedRegion colocatedRegion : colocatedRegionsMap.values()) {
      
      //Fix for 49340 - make sure all colocated regions are initialized, so
      //that we know the region has recovered from disk
      if(!colocatedRegion.isInitialized()) {
        return false;
      }
      if (colocatedRegion.getColocatedWith() == null) {
        colocatedRegions.addFirst(colocatedRegion);
      }
      else {
        colocatedRegions.addLast(colocatedRegion);
      }
    }
    this.colocatedRegions = colocatedRegions;

    return true;
  }

  /**
   * For FPR we will creatd buckets and make primaries as specified by FixedPartitionAttributes.
   * We have to just create buckets and make primaries for the local node. 
   * @return the details of the rebalance.
   */
  public Set<PartitionRebalanceInfo> executeFPA() {
    if (logger.isDebugEnabled()) {
      logger.debug("Rebalancing buckets for fixed partitioned region {}", this.targetRegion);
    }
    
    long start = System.nanoTime();
    GemFireCacheImpl cache = (GemFireCacheImpl)leaderRegion.getCache();
    InternalResourceManager resourceManager = InternalResourceManager
        .getInternalResourceManager(cache);
    InternalResourceManager.getResourceObserver().recoveryStarted(targetRegion);
    try {
      if(!checkAndSetColocatedRegions()) {
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
        details.setPartitionMemberDetailsBefore(model
            .getPartitionedMemberDetails(details.getRegionPath()));
      }

      /*
       * We haen't taken the distributed recovery lock as only this node is
       * creating bucket for itself. It will take bucket creation lock anyway.
       * To move primary too, it has to see what all bucket it can host as
       * primary and make them. We don't need to do all the calculation for fair
       * balance between the nodes as this is a fixed partitioned region.
       */

      if (logger.isDebugEnabled()) {
        logger.debug("Rebalancing FPR {} Model:{}\n", leaderRegion, model);
      }

      director.initialize(model);
      //This will perform all of the required operations.
      director.nextStep();
      
      if (logger.isDebugEnabled()) {
        logger.debug("Rebalancing FPR {} complete. Model:{}\n", leaderRegion, model);
      }
      long end = System.nanoTime();

      for (PartitionRebalanceDetailsImpl details : operator.getDetailSet()) {
        if (!simulate) {
          details.setTime(end - start);
        }
        details.setPartitionMemberDetailsAfter(model
            .getPartitionedMemberDetails(details.getRegionPath()));
      }

      return Collections.<PartitionRebalanceInfo> unmodifiableSet(operator
          .getDetailSet());
    } finally {
      try {
        InternalResourceManager.getResourceObserver().recoveryFinished(
            targetRegion);
      } catch (Exception e) {
        logger.debug(LocalizedMessage.create(LocalizedStrings.PartitionedRegionRebalanceOp_ERROR_IN_RESOURCE_OBSERVER), e);
      }
    }
  }

  private Map<PartitionedRegion, InternalPRInfo> fetchDetails(
      GemFireCacheImpl cache) {
    LoadProbe probe = cache.getResourceManager().getLoadProbe();
    Map<PartitionedRegion, InternalPRInfo> detailsMap = 
      new LinkedHashMap<PartitionedRegion, InternalPRInfo>(
        colocatedRegions.size());
    for (PartitionedRegion colocatedRegion: colocatedRegions) {
      if (ColocationHelper.isColocationComplete(colocatedRegion)) {
        InternalPRInfo info = colocatedRegion.getRedundancyProvider().buildPartitionedRegionInfo(true, probe);
        detailsMap.put(colocatedRegion, info);
      }
    }
    return detailsMap;
  }

  private BucketOperatorWrapper getBucketOperator(
      Map<PartitionedRegion, InternalPRInfo> detailsMap) {
    Set<PartitionRebalanceDetailsImpl> rebalanceDetails = 
      new HashSet<PartitionRebalanceDetailsImpl>(detailsMap.size());
    for (Map.Entry<PartitionedRegion, InternalPRInfo> entry: detailsMap.entrySet()) {
      rebalanceDetails.add(new PartitionRebalanceDetailsImpl(entry.getKey()));
    }
    BucketOperator operator = simulate ? 
        new SimulatedBucketOperator() 
        : new BucketOperatorImpl();
    BucketOperatorWrapper wrapper = new BucketOperatorWrapper(
        operator, rebalanceDetails);
    return wrapper;
  }

  /**
   * Build a model of the load on the partitioned region, which can determine 
   * which buckets to move, etc.
   * @param detailsMap 
   * @param resourceManager 
   */
  private PartitionedRegionLoadModel buildModel(BucketOperator operator, 
      Map<PartitionedRegion, InternalPRInfo> detailsMap, InternalResourceManager resourceManager) {
    PartitionedRegionLoadModel model;
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    final DM dm = leaderRegion.getDistributionManager();    
    AddressComparor comparor = new AddressComparor() {
      
      public boolean areSameZone(InternalDistributedMember member1,
          InternalDistributedMember member2) {
        return dm.areInSameZone(member1, member2);
      }

      public boolean enforceUniqueZones() {
        return dm.enforceUniqueZone();
      }
    };
    
    
    int redundantCopies = leaderRegion.getRedundantCopies();
    int totalNumberOfBuckets = leaderRegion.getTotalNumberOfBuckets();
    Set<InternalDistributedMember> criticalMembers = resourceManager.getResourceAdvisor().adviseCritialMembers();;
    boolean removeOverRedundancy = true;
    model = new PartitionedRegionLoadModel(operator, redundantCopies, 
        totalNumberOfBuckets, comparor, criticalMembers, leaderRegion);

    for (Map.Entry<PartitionedRegion, InternalPRInfo> entry : detailsMap.entrySet()) {
      PartitionedRegion region = entry.getKey();
      InternalPRInfo details = entry.getValue();
      if (isDebugEnabled) {
        logger.debug("Added Region to model region={} details=", region);
      }
      for(PartitionMemberInfo memberDetails: details.getPartitionMemberInfo()) {
        if (isDebugEnabled) {
          logger.debug("Member: {} LOAD={}", memberDetails.getDistributedMember(), ((InternalPartitionDetails) memberDetails).getPRLoad());
        }
      }
      Set<InternalPartitionDetails> memberDetailSet = 
          details.getInternalPartitionDetails();
      OfflineMemberDetails offlineDetails;
      if(replaceOfflineData) {
        offlineDetails = OfflineMemberDetails.EMPTY_DETAILS;
      } else {
        offlineDetails = details.getOfflineMembers();
      }
      boolean enforceLocalMaxMemory = !region.isEntryEvictionPossible();
      model.addRegion(region.getFullPath(), memberDetailSet, offlineDetails, enforceLocalMaxMemory);
    }
    
    model.initialize();
    
    return model;
  }
  /**
   * Create a redundant bucket on the target member
   * 
   * @param target
   *          the member on which to create the redundant bucket
   * @param bucketId
   *          the identifier of the bucket
   * @param pr
   *          the partitioned region which contains the bucket
   * @param forRebalance
   *          true if part of a rebalance operation
   * @return true if the redundant bucket was created
   */
  public static boolean createRedundantBucketForRegion(
      InternalDistributedMember target, int bucketId, PartitionedRegion pr,
      boolean forRebalance, boolean replaceOfflineData) {
    return pr.getRedundancyProvider().createBackupBucketOnMember(bucketId,
        target, forRebalance, replaceOfflineData,null, true);
  }
  
  /**
   * Remove a redundant bucket on the target member
   * 
   * @param target
   *          the member on which to create the redundant bucket
   * @param bucketId
   *          the identifier of the bucket
   * @param pr
   *          the partitioned region which contains the bucket
   * @return true if the redundant bucket was removed
   */
  public static boolean removeRedundantBucketForRegion(
      InternalDistributedMember target, int bucketId, PartitionedRegion pr) {
    boolean removed = false;
    if (pr.getDistributionManager().getId().equals(target)) {
      // invoke directly on local member...
      removed = pr.getDataStore().removeBucket(bucketId, false);
    }
    else {
      // send message to remote member...
      RemoveBucketResponse response = RemoveBucketMessage.send(target, pr,
          bucketId, false);
      if (response != null) {
        removed = response.waitForResponse();
      }
    }
    return removed;
  }

  /**
   * Move the primary of a bucket to the target member
   * 
   * @param target
   *          the member which should be primary
   * @param bucketId
   *          the identifier of the bucket
   * @param pr
   *          the partitioned region which contains the bucket
   * @param forRebalance
   *          true if part of a rebalance operation
   * @return true if the move was successful
   */
  public static boolean movePrimaryBucketForRegion(
      InternalDistributedMember target, int bucketId, PartitionedRegion pr,
      boolean forRebalance) {
    boolean movedPrimary = false;
    if (pr.getDistributionManager().getId().equals(target)) {
      // invoke directly on local member...
      BucketAdvisor bucketAdvisor = pr.getRegionAdvisor().getBucketAdvisor(
          bucketId);
      if (bucketAdvisor.isHosting()) {
        movedPrimary = bucketAdvisor.becomePrimary(forRebalance);
      }
    }
    else {
      // send message to remote member...
      BecomePrimaryBucketResponse response = BecomePrimaryBucketMessage.send(
          target, pr, bucketId, forRebalance);
      if (response != null) {
        movedPrimary = response.waitForResponse();
      }
    }
    return movedPrimary;
  }

  /**
   * Move a bucket from the provided source to the target
   * 
   * @param source
   *          member that contains the bucket
   * @param target
   *          member which should receive the bucket
   * @param bucketId
   *          the identifier of the bucket
   * @param pr
   *          the partitioned region which contains the bucket
   * @return true if the bucket was moved
   */
  public static boolean moveBucketForRegion(InternalDistributedMember source,
      InternalDistributedMember target, int bucketId, PartitionedRegion pr) {
    boolean movedBucket = false;
    if (pr.getDistributionManager().getId().equals(target)) {
      // invoke directly on local member...
      movedBucket = pr.getDataStore().moveBucket(bucketId, source, false);
    }
    else {
      // send message to remote member...
      MoveBucketResponse response = MoveBucketMessage.send(target, pr,
          bucketId, source);
      if (response != null) {
        movedBucket = response.waitForResponse();
      }
    }
    return movedBucket;
  } 
  private boolean isRebalanceNecessary() {
    //Fixed partitions will always be rebalanced.
    //Persistent partitions that have recovered from disk will
    //also need to rebalance primaries
    return isRebalance 
        || director.isRebalanceNecessary(leaderRegion.getRedundancyProvider().isRedundancyImpaired(), 
           leaderRegion.getDataPolicy().withPersistence());
  }
  
  private class MembershipChangeListener implements MembershipListener {

    public void memberDeparted(InternalDistributedMember id, boolean crashed) {
      if (logger.isDebugEnabled()) {
        logger.debug("PartitionedRegionRebalanceOP - membership changed, restarting rebalancing for region {}", targetRegion);
      }
      membershipChange = true;
    }

    public void memberJoined(InternalDistributedMember id) {
      if (logger.isDebugEnabled()) {
        logger.debug("PartitionedRegionRebalanceOP - membership changed, restarting rebalancing for region {}", targetRegion);
      }
      membershipChange = true;
    }

    public void memberSuspect(InternalDistributedMember id,
        InternalDistributedMember whoSuspected) {
      // do nothing.
    }
    
    public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
    }
  }
  
  private class BucketOperatorImpl implements BucketOperator {

    @Override
    public boolean moveBucket(InternalDistributedMember source,
        InternalDistributedMember target, int bucketId,
        Map<String, Long> colocatedRegionBytes) {

      InternalResourceManager.getResourceObserver().movingBucket(
          leaderRegion, bucketId, source, target);
      return moveBucketForRegion(source, target, bucketId, leaderRegion);
    }

    @Override
    public boolean movePrimary(InternalDistributedMember source,
        InternalDistributedMember target, int bucketId) {

      InternalResourceManager.getResourceObserver().movingPrimary(
          leaderRegion, bucketId, source, target);
      return movePrimaryBucketForRegion(target, bucketId, leaderRegion, isRebalance); 
    }

    @Override
    public void createRedundantBucket(
        InternalDistributedMember targetMember, int bucketId,
        Map<String, Long> colocatedRegionBytes, Completion completion) {
      boolean result = false;
      try {
        result = createRedundantBucketForRegion(targetMember, bucketId,
          leaderRegion, isRebalance,replaceOfflineData);
      } finally {
        if(result) {
          completion.onSuccess();
        } else {
          completion.onFailure();
        }
      }
    }
    
    @Override
    public void waitForOperations() {
      //do nothing, all operations are synchronous
    }

    @Override
    public boolean removeBucket(InternalDistributedMember targetMember, int bucketId,
        Map<String, Long> colocatedRegionBytes) {
      return removeRedundantBucketForRegion(targetMember, bucketId,
          leaderRegion);
    }
  }

  /**
   * A wrapper class which delegates actual bucket operations to the enclosed BucketOperator,
   * but keeps track of statistics about how many buckets are created, transfered, etc.
   * @author dsmith
   *
   */
  private class BucketOperatorWrapper implements 
      BucketOperator {
    private final BucketOperator delegate;
    private final Set<PartitionRebalanceDetailsImpl> detailSet;
    private final int regionCount;
  
    public BucketOperatorWrapper(
        BucketOperator delegate,
        Set<PartitionRebalanceDetailsImpl> rebalanceDetails) {
      this.delegate = delegate;
      this.detailSet = rebalanceDetails;
      this.regionCount = detailSet.size();
    }
    @Override
    public boolean moveBucket(InternalDistributedMember sourceMember,
        InternalDistributedMember targetMember, int id,
        Map<String, Long> colocatedRegionBytes) {
      long start = System.nanoTime();
      boolean result = false;
      long elapsed = 0;
      long totalBytes = 0;


      if (stats != null) {
        stats.startBucketTransfer(regionCount);
      }
      try {
        result = delegate.moveBucket(sourceMember, targetMember, id,
            colocatedRegionBytes);
        elapsed = System.nanoTime() - start;
        if (result) {
          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} bucket {} moved from {} to {}", leaderRegion, id, sourceMember, targetMember);
          }
          for (PartitionRebalanceDetailsImpl details : detailSet) {
            String regionPath = details.getRegionPath();
            Long regionBytes = colocatedRegionBytes.get(regionPath);
            if(regionBytes != null) {
            //only increment the elapsed time for the leader region
              details.incTransfers(regionBytes.longValue(),
                  details.getRegion().equals(leaderRegion) ? elapsed : 0);
              totalBytes += regionBytes.longValue();
            }
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} bucket {} moved failed from {} to {}", leaderRegion, id, sourceMember, targetMember);
          }
        }
      } finally {
        if(stats != null) {
          stats.endBucketTransfer(regionCount, result, totalBytes, elapsed);
        }
      }
      
      return result;
    }

    @Override
    public void createRedundantBucket(
        final InternalDistributedMember targetMember, final int i, 
        final Map<String, Long> colocatedRegionBytes, final Completion completion) {
      
      if(stats != null) {
        stats.startBucketCreate(regionCount);
      }
      
      final long start = System.nanoTime();
      delegate.createRedundantBucket(targetMember, i,  
          colocatedRegionBytes, new Completion() {

        @Override
        public void onSuccess() {
          long totalBytes = 0;
          long elapsed= System.nanoTime() - start;
          if(logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} redundant bucket {} created on {}", leaderRegion, i, targetMember);
          }
          for (PartitionRebalanceDetailsImpl details : detailSet) {
            String regionPath = details.getRegionPath();
            Long lrb = colocatedRegionBytes.get(regionPath);
            if (lrb != null) { // region could have gone away - esp during shutdow
              long regionBytes = lrb.longValue();
              //Only add the elapsed time to the leader region.
              details.incCreates(regionBytes, 
                  details.getRegion().equals(leaderRegion) ? elapsed : 0);
              totalBytes += regionBytes;
            }
          }

          if(stats != null) {
            stats.endBucketCreate(regionCount, true, totalBytes, elapsed);
          }

        }

        @Override
        public void onFailure() {
          long elapsed= System.nanoTime() - start;

          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} redundant bucket {} failed creation on {}", leaderRegion, i, targetMember);
          }

          if(stats != null) {
            stats.endBucketCreate(regionCount, false, 0, elapsed);
          }
        }
      });
    }
    
    @Override
    public boolean removeBucket(
        InternalDistributedMember targetMember, int i, 
        Map<String, Long> colocatedRegionBytes) {
      boolean result = false;
      long elapsed = 0;
      long totalBytes = 0;
      
      
      if(stats != null) {
        stats.startBucketRemove(regionCount);
      }
      try {
        long start = System.nanoTime();
        result = delegate.removeBucket(targetMember, i,  
            colocatedRegionBytes);
        elapsed= System.nanoTime() - start;
        if (result) {
          if(logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} redundant bucket {} removed from {}", leaderRegion, i, targetMember);
          }
          for (PartitionRebalanceDetailsImpl details : detailSet) {
            String regionPath = details.getRegionPath();
            Long lrb = colocatedRegionBytes.get(regionPath);
            if (lrb != null) { // region could have gone away - esp during shutdow
              long regionBytes = lrb.longValue();
              //Only add the elapsed time to the leader region.
              details.incRemoves(regionBytes, 
                  details.getRegion().equals(leaderRegion) ? elapsed : 0);
              totalBytes += regionBytes;
            }
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} redundant bucket {} failed removal o{}", leaderRegion, i, targetMember);
          }
        }
      } finally {
        if(stats != null) {
          stats.endBucketRemove(regionCount, result, totalBytes, elapsed);
        }
      }
      
      return result;
    }
  
    @Override
    public boolean movePrimary(InternalDistributedMember source,
        InternalDistributedMember target, int bucketId) {
      boolean result = false;
      long elapsed = 0;
      
      if(stats != null) {
        stats.startPrimaryTransfer(regionCount);
      }

      try {
        long start = System.nanoTime();
        result = delegate.movePrimary(source, target, bucketId);
        elapsed = System.nanoTime() - start;
        if (result) {
          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} primary bucket {} moved from {} to {}", leaderRegion, bucketId, source, target);
          }
          for (PartitionRebalanceDetailsImpl details : detailSet) {
            details.incPrimaryTransfers(details.getRegion().equals(leaderRegion) ? elapsed : 0);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Rebalancing {} primary bucket {} failed to move from {} to {}", leaderRegion, bucketId, source, target);
          }
        }
    } finally {
      if(stats != null) {
        stats.endPrimaryTransfer(regionCount, result, elapsed);
      }
    }
      
      return result;
    }
    
    @Override
    public void waitForOperations() {
      delegate.waitForOperations();
    }

    public Set<PartitionRebalanceDetailsImpl> getDetailSet() {
      return this.detailSet;
    }
  }
}
