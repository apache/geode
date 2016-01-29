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
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.partitioned.InternalPartitionDetails;
import com.gemstone.gemfire.internal.cache.partitioned.OfflineMemberDetails;
import com.gemstone.gemfire.internal.cache.partitioned.PRLoad;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionMemberInfoImpl;
import com.gemstone.gemfire.internal.cache.persistence.PersistentMemberID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * A model of the load on all of the members for a partitioned region. This
 * model is used to find the best members to create buckets on or move buckets
 * or primaries too. All of the actual work of creating a copy, moving a
 * primary, etc. Is performed by the BucketOperator that is passed to the
 * constructor.
 * 
 * To use, create a model and populate it using the addMember method. addMember
 * takes a region argument, to indicate which region the data is for. All of the
 * regions added to a single model are assumed to be colocated, and the model
 * adds together the load from each of the individual regions to balance all of
 * the regions together.
 * 
 * Reblancing operations are performed by repeatedly calling model.nextStep
 * until it returns false. Each call to nextStep should perform another
 * operation. The model will make callbacks to the BucketOperator you provide to
 * the contructor perform the actual create or move.
 * 
 * While creating redundant copies our moving buckets, this model tries to
 * minimize the standard deviation in the weighted loads for the members. The
 * weighted load for the member is the sum of the load for all of the buckets on
 * the member divided by that members weight.
 * 
 * This model is not threadsafe.
 * 
 * @author dsmith
 * @since 6.0
 * 
 */
@SuppressWarnings("synthetic-access")
public class PartitionedRegionLoadModel {
  private static Logger logger = LogService.getLogger();
  
  /**
   * A comparator that is used to sort buckets in the order
   * that we should satisfy redundancy - most needy buckets first.
   */
  private static final Comparator<Bucket> REDUNDANCY_COMPARATOR = new Comparator<Bucket>() {
    public int compare(Bucket o1, Bucket o2) {
      // put the buckets with the lowest redundancy first
      int result = o1.getRedundancy() - o2.getRedundancy();
      if (result == 0) {
        // put the bucket with the largest load first. This should give us a
        // better chance of finding a place to put it
        result = Float.compare(o2.getLoad(), o1.getLoad());
      }
      if (result == 0) {
        // finally, just use the id so the comparator doesn't swallow buckets
        // with the same load
        result = o1.getId() - o2.getId();
      }

      return result;
    }
  };

  private static final long MEGABYTES = 1024 * 1024;

  /**
   * A member to represent inconsistent data. For example, if two members think
   * they are the primary for a bucket, we will set the primary to invalid, so it won't
   * be a candidate for rebalancing.
   */
  final MemberRollup INVALID_MEMBER = new MemberRollup(null, false, false);
  
  private final BucketRollup[] buckets;
  
  /**
   * A map of all members that host this partitioned region
   */
  private final Map<InternalDistributedMember, MemberRollup> members = new HashMap<InternalDistributedMember, MemberRollup>();
  
  /**
   * The set of all regions that are colocated in this model.
   */
  private final Set<String> allColocatedRegions= new HashSet<String>();
  
  /**
   * The list of buckets that have low redundancy
   */
  private SortedSet<BucketRollup> lowRedundancyBuckets= null;
  private SortedSet<BucketRollup> overRedundancyBuckets= null;
  private final Collection<Move> attemptedPrimaryMoves = new HashSet<Move>();
  private final Collection<Move> attemptedBucketMoves = new HashSet<Move>();
  private final Collection<Move> attemptedBucketCreations = new HashSet<Move>();
  private final Collection<Move> attemptedBucketRemoves = new HashSet<Move>();
  
  private final BucketOperator operator;
  private final int requiredRedundancy;

  /**The average primary load on a member */
  private float primaryAverage = -1;
  /**The average bucket load on a member */
  private float averageLoad = -1;
  /**
   * The minimum improvement in variance that we'll consider worth moving a
   * primary
   */
  private double minPrimaryImprovement = -1;
  /**
   * The minimum improvement in variance that we'll consider worth moving a
   * bucket
   */
  private double minImprovement = -1;

  private final AddressComparor addressComparor;

  private final Set<InternalDistributedMember> criticalMembers;

  private final PartitionedRegion partitionedRegion;

  
  /**
   * Create a new model
   * @param operator the operator which performs the actual creates/moves for buckets
   * @param redundancyLevel The expected redundancy level for the region
   */
  public PartitionedRegionLoadModel(BucketOperator operator,
      int redundancyLevel, int numBuckets, AddressComparor addressComparor,
      Set<InternalDistributedMember> criticalMembers, PartitionedRegion region) {
    this.operator = operator;
    this.requiredRedundancy = redundancyLevel;
    this.buckets = new BucketRollup[numBuckets];
    this.addressComparor = addressComparor;
    this.criticalMembers = criticalMembers;
    this.partitionedRegion = region;
  }
  
  /**
   * Add a region to the model. All regions that are added are assumed to be colocated.
   * The first region added to the model should be the parent region. The parent region
   * is expected to have at least as many members as child regions; it may have more. If
   * the parent has more members than child regions those members will be considered invalid.
   * @param region
   * @param memberDetailSet A set of details about each member.
   * @param offlineDetails 
   */
  public void addRegion(String region,
      Collection<? extends InternalPartitionDetails> memberDetailSet, 
      OfflineMemberDetails offlineDetails,
      boolean enforceLocalMaxMemory) {
    this.allColocatedRegions.add(region);
    //build up a list of members and an array of buckets for this
    //region. Each bucket has a reference to all of the members
    //that host it and each member has a reference to all of the buckets
    //it hosts
    Map<InternalDistributedMember, Member> regionMember = new HashMap<InternalDistributedMember, Member>();
    Bucket[] regionBuckets = new Bucket[this.buckets.length];
    for(InternalPartitionDetails memberDetails : memberDetailSet) {
      InternalDistributedMember memberId = (InternalDistributedMember) memberDetails.getDistributedMember();

      boolean isCritical = criticalMembers.contains(memberId);
      Member member = new Member(memberId,
          memberDetails.getPRLoad().getWeight(), memberDetails.getConfiguredMaxMemory(), isCritical, enforceLocalMaxMemory);
      regionMember.put(memberId, member);

      PRLoad load = memberDetails.getPRLoad();
      for(int i = 0; i < regionBuckets.length; i++) {
        if(load.getReadLoad(i) > 0) {
          Bucket bucket = regionBuckets[i];
          if(bucket == null) {
            Set<PersistentMemberID> offlineMembers = offlineDetails.getOfflineMembers(i);
            bucket = new Bucket(i, load.getReadLoad(i), memberDetails.getBucketSize(i), offlineMembers);
            regionBuckets[i] = bucket;
          }
          bucket.addMember(member);
          if(load.getWriteLoad(i) > 0) {
            if(bucket.getPrimary() == null) {
              bucket.setPrimary(member, load.getWriteLoad(i));
            } else if(!bucket.getPrimary().equals(member)) {
              bucket.setPrimary(INVALID_MEMBER, 1);
            }
          }
        }
      }
    }
    
    //add each member for this region to a rollup of all colocated
    //regions
    for(Member member: regionMember.values()) {
      InternalDistributedMember memberId = member.getDistributedMember();
      MemberRollup memberSum = this.members.get(memberId);
      boolean isCritical = criticalMembers.contains(memberId);
      if(memberSum == null) {
        memberSum = new MemberRollup(memberId, isCritical, enforceLocalMaxMemory);
        this.members.put(memberId, memberSum);
      } 
      
      memberSum.addColocatedMember(region, member);
    }
    
    //Now, add the region to the rollups of the colocated
    //regions and buckets
    for(int i =0; i < this.buckets.length; i++) {
      if(regionBuckets[i] == null) {
        //do nothing, this bucket is not hosted for this region.
        // [sumedh] remove from buckets array too to be consistent since
        // this method will be invoked repeatedly for all colocated regions,
        // and then we may miss some colocated regions for a bucket leading
        // to all kinds of issues later (e.g. see SQLF test for #41472 that
        //   shows some problems including NPEs, hangs etc.)
        this.buckets[i] = null;
        continue;
      }
      if(this.buckets[i]==null) {
        //If this is the first region we have seen that is hosting this bucket, create a bucket rollup
        this.buckets[i] = new BucketRollup(i);
      }
      
      //Add all of the members hosting the bucket to the rollup
      for(Member member: regionBuckets[i].getMembersHosting()) {
        InternalDistributedMember memberId = member.getDistributedMember();
        this.buckets[i].addMember(this.members.get(memberId));
      }
      
      //set the primary for the rollup
      if(regionBuckets[i].getPrimary() != null) {
        if(this.buckets[i].getPrimary() == null) {
          InternalDistributedMember memberId = regionBuckets[i].getPrimary().getDistributedMember();
          this.buckets[i].setPrimary(this.members.get(memberId), 0);
        }
        else{
          if(!(this.buckets[i].getPrimary() == INVALID_MEMBER)){
            if (!this.buckets[i].getPrimary().getDistributedMember().equals(
                regionBuckets[i].getPrimary().getDistributedMember())) {
              if (logger.isDebugEnabled()) {
                logger.debug("PartitionedRegionLoadModel - Setting bucket {} to INVALID because it is the primary on two members.This could just be a race in the collocation of data. member1={} member2={}",
                    this.buckets[i], this.buckets[i].getPrimary(), regionBuckets[i].getPrimary());
              }
              this.buckets[i].setPrimary(INVALID_MEMBER, 0);
            }
          }
        }
      }
      this.buckets[i].addColocatedBucket(region, regionBuckets[i]);
    }
    
    //TODO rebalance - there is a possibility of adding members
    //back here, which I don't like. I think maybe all of the regions should be in the
    //constructor for the load model, and then when the constructor is done
    //we can do with validation.
    //If any members don't have this new region, remove them.
    for(Iterator<Entry<InternalDistributedMember, MemberRollup>> itr = members.entrySet().iterator();itr.hasNext();) {
      MemberRollup memberRollup = itr.next().getValue();
      if(!memberRollup.getColocatedMembers().keySet().equals(this.allColocatedRegions)) {
        itr.remove();
        if(logger.isDebugEnabled()) {
          logger.debug("PartitionedRegionLoadModel - removing member {} from the consideration because it doesn't have all of the colocated regions. Expected={}, was={}",
              memberRollup, allColocatedRegions, memberRollup.getColocatedMembers());
        }
        //This state should never happen
        if(!memberRollup.getBuckets().isEmpty()) {
          logger.warn(LocalizedMessage.create(LocalizedStrings.PartitionedRegionLoadModel_INCOMPLETE_COLOCATION,
              new Object[] { memberRollup, this.allColocatedRegions, memberRollup.getColocatedMembers().keySet(), memberRollup.getBuckets() }));
        }
        for(Bucket bucket: new HashSet<Bucket>(memberRollup.getBuckets())) {
          bucket.removeMember(memberRollup);
        }
      }
    }
  }
  
  public void initialize() {
    resetAverages();
    initOverRedundancyBuckets();
    initLowRedundancyBuckets();
  }
  
  public SortedSet<BucketRollup> getLowRedundancyBuckets() {
    return lowRedundancyBuckets;
  }
  
  public SortedSet<BucketRollup> getOverRedundancyBuckets() {
    return overRedundancyBuckets;
  }

  public void setOverRedundancyBuckets(
      SortedSet<BucketRollup> overRedundancyBuckets) {
    this.overRedundancyBuckets = overRedundancyBuckets;
  }

  public boolean enforceUniqueZones() {
    return addressComparor.enforceUniqueZones();
  }
  
  public void ignoreLowRedundancyBucket(BucketRollup first) {
    this.lowRedundancyBuckets.remove(first);
  }
  
  public void ignoreOverRedundancyBucket(BucketRollup first) {
    this.overRedundancyBuckets.remove(first);
  }
  
  public MemberRollup getMember(InternalDistributedMember target) {
    return members.get(target);
  }
  
  public BucketRollup[] getBuckets() {
    return buckets;
  }
  
  public String getName() {
    return getPartitionedRegion().getFullPath();
  }
  
  public PartitionedRegion getPartitionedRegion() {
    //TODO - this model really should not have
    //a reference to the partitioned region object.
    //The fixed PR code currently depends on this
    //partitioned region object and needs
    //refactoring.
    return partitionedRegion;
  }

  private Map<String, Long> getColocatedRegionSizes(BucketRollup bucket) {
    Map<String, Long> colocatedRegionSizes = new HashMap<String, Long>();
    
    for (Map.Entry<String, Bucket> entry : bucket.getColocatedBuckets()
        .entrySet()) {
      colocatedRegionSizes.put(entry.getKey(),
          Long.valueOf(entry.getValue().getBytes()));
    }
    return colocatedRegionSizes;
  }

  /**
   * Trigger the creation of a redundant bucket, potentially asynchronously.
   * 
   * This method will find the best node to create a redundant bucket and 
   * invoke the bucket operator to create a bucket on that node. Because the bucket
   * operator is asynchronous, the bucket may not be created immediately, but
   * the model will be updated regardless. Invoke {@link #waitForOperations()}
   * to wait for those operations to actually complete
   */
  public void createRedundantBucket(final BucketRollup bucket,
      final Member targetMember) {
    Map<String, Long> colocatedRegionSizes = getColocatedRegionSizes(bucket);
    final Move move = new Move(null, targetMember, bucket);
    
      this.lowRedundancyBuckets.remove(bucket);
      bucket.addMember(targetMember);
      //put the bucket back into the list if we still need to satisfy redundancy for
      //this bucket
      if(bucket.getRedundancy() < this.requiredRedundancy) {
        this.lowRedundancyBuckets.add(bucket);
      }
      resetAverages();
    
    this.operator.createRedundantBucket(targetMember.getMemberId(), bucket.getId(), colocatedRegionSizes, new BucketOperator.Completion() {
      @Override
      public void onSuccess() {
    }

      @Override
      public void onFailure() {
        //If the bucket creation failed, we need to undo the changes
        //we made to the model
        attemptedBucketCreations.add(move);
        bucket.removeMember(targetMember);
        if(bucket.getRedundancy() < requiredRedundancy) {
          lowRedundancyBuckets.add(bucket);
        }
        resetAverages();
      }
    });
  }
  
  

  protected void remoteOverRedundancyBucket(BucketRollup bucket,
      Member targetMember) {
    
    Move bestMove = new Move(null, targetMember, bucket);
    Map<String, Long> colocatedRegionSizes = getColocatedRegionSizes(bucket);
    
    if(!this.operator.removeBucket(targetMember.getMemberId(), bucket.getId(), colocatedRegionSizes)) {
      this.attemptedBucketRemoves.add(bestMove);
    } else {
      this.overRedundancyBuckets.remove(bucket);
      bucket.removeMember(targetMember);
      //put the bucket back into the list if we still need to satisfy redundancy for
      //this bucket
      if(bucket.getOnlineRedundancy() > this.requiredRedundancy) {
        this.overRedundancyBuckets.add(bucket);
      }
      resetAverages();
    }
  }
  
  private void initLowRedundancyBuckets() {
    this.lowRedundancyBuckets = new TreeSet<BucketRollup>(REDUNDANCY_COMPARATOR);
    for(BucketRollup b: this.buckets) {
      if(b != null && b.getRedundancy() >= 0 && b.getRedundancy() < this.requiredRedundancy) {
        this.lowRedundancyBuckets.add(b);
      }
    }
  }
  
  private void initOverRedundancyBuckets() {
    this.overRedundancyBuckets = new TreeSet<BucketRollup>(REDUNDANCY_COMPARATOR);
    for(BucketRollup b: this.buckets) {
      if(b != null && b.getOnlineRedundancy() > this.requiredRedundancy) {
        this.overRedundancyBuckets.add(b);
      }
    }
  }

  /**
   * Find the best member to put a new bucket on.
   * 
   * @param bucket
   *          the bucket we want to create
   * @param checkIPAddress
   *          true if we should only consider members that do not have the same
   *          IP Address as a member that already hosts the bucket
   */
  public Move findBestTarget(Bucket bucket, boolean checkIPAddress) {
    float leastCost = Float.MAX_VALUE;
    Move bestMove = null;
    
    for (Member member : this.members.values()) {
      if (member.willAcceptBucket(bucket, null, checkIPAddress).willAccept()) {
        float cost = (member.getTotalLoad() + bucket.getLoad())
            / member.getWeight();
        if (cost < leastCost) {
          Move move = new Move(null, member, bucket);
          if (!this.attemptedBucketCreations.contains(move)) {
            leastCost = cost;
            bestMove = move;
          }
        }
      }
    }
    return bestMove;
  }
  
  /**
   * Find the best member to remove a bucket from
   * 
   * @param bucket
   *          the bucket we want to create
   */
  public Move findBestRemove(Bucket bucket) {
    float mostLoaded = Float.MIN_VALUE;
    Move bestMove = null;
    
    for (Member member : bucket.getMembersHosting()) {
      float newLoad = (member.getTotalLoad() - bucket.getLoad())
      / member.getWeight();
      if (newLoad > mostLoaded && ! member.equals(bucket.getPrimary())) {
        Move move = new Move(null, member, bucket);
        if (!this.attemptedBucketRemoves.contains(move)) {
          mostLoaded = newLoad;
          bestMove = move;
        }
      }
    }
    return bestMove;
  }

  public Move findBestTargetForFPR(Bucket bucket, boolean checkIPAddress) {
    Move noMove = null;
    InternalDistributedMember targetMemberID = null;
    Member targetMember = null;
    List<FixedPartitionAttributesImpl> fpas = this.partitionedRegion
        .getFixedPartitionAttributesImpl();
    
    if (fpas != null) {
      for (FixedPartitionAttributesImpl fpaImpl : fpas) {
        if (fpaImpl.hasBucket(bucket.getId())) {
          targetMemberID = this.partitionedRegion.getDistributionManager()
              .getDistributionManagerId();
          if (this.members.containsKey(targetMemberID)) {
            targetMember = this.members.get(targetMemberID);
            if (targetMember.willAcceptBucket(bucket, null, checkIPAddress).willAccept()) {
              // We should have just one move for creating
              // all the buckets for a FPR on this node.
              return new Move(null, targetMember, bucket);
            }
          }
        }
      }
    }
    
    return noMove;
  }

  protected boolean movePrimary(Move bestMove) {
    Member bestSource = bestMove.getSource();
    Member bestTarget = bestMove.getTarget();
    Bucket bestBucket = bestMove.getBucket();
    boolean successfulMove = this.operator.movePrimary(bestSource.getDistributedMember(), bestTarget
        .getDistributedMember(), bestBucket.getId());

    if(successfulMove) {
      bestBucket.setPrimary(bestTarget, bestBucket.getPrimaryLoad());
    }

    boolean entryAdded  = this.attemptedPrimaryMoves.add(bestMove);
    Assert
    .assertTrue(entryAdded,
        "PartitionedRegionLoadModel.movePrimarys - excluded set is not growing, so we probably would have an infinite loop here");
    
    return successfulMove;
  }

  public Move findBestPrimaryMove() {
    Move bestMove= null;
    double bestImprovement = 0;
    GemFireCacheImpl cache = null;
    for(Member source: this.members.values()) {
      for(Bucket bucket: source.getPrimaryBuckets()) {
        for(Member target: bucket.getMembersHosting()) {
          if(source.equals(target)) {
            continue;
          }
          // If node is not fully initialized yet, then skip this node
          // (SQLFabric DDL replay in progress).
          if (cache == null) {
            cache = GemFireCacheImpl.getInstance();
          }
          if (cache != null
              && cache.isUnInitializedMember(target.getMemberId())) {
            continue;
          }
          double improvement = improvement(source.getPrimaryLoad(), source
              .getWeight(), target.getPrimaryLoad(), target.getWeight(), bucket.getPrimaryLoad(),
              getPrimaryAverage());
          if (improvement > bestImprovement && improvement > getMinPrimaryImprovement()) {
            Move move = new Move(source, target, bucket);
            if (!this.attemptedPrimaryMoves.contains(move)) {
              bestImprovement = improvement;
              bestMove = move;
            }
          }
        }
      }
    }
    return bestMove;
  }

  /**
   * Move all primary from other to this 
   */
  private void makeFPRPrimaryForThisNode() {
    List<FixedPartitionAttributesImpl> FPAs = this.partitionedRegion
        .getFixedPartitionAttributesImpl();
    InternalDistributedMember targetId = this.partitionedRegion
        .getDistributionManager().getId();
    Member target = this.members.get(targetId);
    Set<Bucket> unsuccessfulAttempts = new HashSet<Bucket>();
    for (Bucket bucket : this.buckets) {
      if (bucket != null) {
        for (FixedPartitionAttributesImpl fpa : FPAs) {
          if (fpa.hasBucket(bucket.id) && fpa.isPrimary()) {
            Member source = bucket.primary;
            bucket.getPrimary();
            if (source != target) {
              // HACK: In case we don't know who is Primary at this time
              // we just set source as target too for stat purposes

              InternalDistributedMember srcDM = (source == null || source == INVALID_MEMBER) ? target
                  .getDistributedMember() : source.getDistributedMember();
              if (logger.isDebugEnabled()) {
                logger.debug("PRLM#movePrimariesForFPR: For Bucket#{}, moving primary from source {} to target {}",
                    bucket.getId(), bucket.primary, target);
              }
              boolean successfulMove = this.operator.movePrimary(srcDM,
                  target.getDistributedMember(), bucket.getId());
              unsuccessfulAttempts.add(bucket);
              // We have to move the primary otherwise there is some problem!
              Assert.assertTrue(successfulMove,
                  " Fixed partitioned region not able to move the primary!");
              if (successfulMove) {
                if (logger.isDebugEnabled()) {
                  logger.debug("PRLM#movePrimariesForFPR: For Bucket#{}, moving primary source {} to target {}",
                      bucket.getId(), source, target);
                }

                bucket.setPrimary(target, bucket.getPrimaryLoad());
              } 
            }
          }
        }
      }
    }
  }

  /**
   * Calculate the target weighted number of primaries on each node.
   */
  private float getPrimaryAverage() {
    if(this.primaryAverage == -1) {
      float totalWeight = 0;
      float totalPrimaryCount = 0;
      for(Member member : this.members.values()) {
        totalPrimaryCount += member.getPrimaryLoad();
        totalWeight += member.getWeight();
      }
      
      this.primaryAverage = totalPrimaryCount / totalWeight;
    }
    
    return this.primaryAverage;
  }
  
  /**
   * Calculate the target weighted amount of data on each node.
   */
  private float getAverageLoad() {
    if(this.averageLoad == -1) {
      float totalWeight = 0;
      float totalLoad = 0;
      for(Member member : this.members.values()) {
        totalLoad += member.getTotalLoad();
        totalWeight += member.getWeight();
      }
      
      this.averageLoad = totalLoad / totalWeight;
    }
    
    return this.averageLoad;
  }

  /**
   * Calculate the minimum improvement in variance that will we consider worth
   * while. Currently this is calculated as the improvement in variance that
   * would occur by removing the smallest bucket from the member with the
   * largest weight.
   */
  private double getMinPrimaryImprovement() {
    if((this.minPrimaryImprovement + 1.0) < .0000001) { // i.e. == -1
      float largestWeight = 0;
      float smallestBucket = 0;
      for(Member member : this.members.values()) {
        if(member.getWeight() > largestWeight) {
          largestWeight = member.getWeight();
        }
        for(Bucket bucket: member.getPrimaryBuckets()) {
          if(bucket.getPrimaryLoad() < smallestBucket || smallestBucket == 0) {
            smallestBucket = bucket.getPrimaryLoad();
          }
        }
      }
      double before = variance(getPrimaryAverage() * largestWeight
          + smallestBucket, largestWeight, getPrimaryAverage());
      double after = variance(getPrimaryAverage() * largestWeight,
          largestWeight, getPrimaryAverage());
      this.minPrimaryImprovement = (before - after) / smallestBucket;
    }
    return this.minPrimaryImprovement;
  }
  
  /**
   * Calculate the minimum improvement in variance that will we consider worth
   * while. Currently this is calculated as the improvement in variance that
   * would occur by removing the smallest bucket from the member with the
   * largest weight.
   */
  private double getMinImprovement() {
    if((this.minImprovement + 1.0) < .0000001) { // i.e. == -1
      float largestWeight = 0;
      float smallestBucket = 0;
      for(Member member : this.members.values()) {
        if(member.getWeight() > largestWeight) {
          largestWeight = member.getWeight();
        }
        //find the smallest bucket, ignoring empty buckets.
        for(Bucket bucket: member.getBuckets()) {
          if(smallestBucket == 0 || (bucket.getLoad() < smallestBucket && bucket.getBytes() > 0) ) {
            smallestBucket = bucket.getLoad();
          }
        }
      }
      double before = variance(getAverageLoad() * largestWeight
          + smallestBucket, largestWeight, getAverageLoad());
      double after = variance(getAverageLoad() * largestWeight,
          largestWeight, getAverageLoad());
      this.minImprovement = (before - after) / smallestBucket;
    }
    return this.minImprovement;
  }
  
  private void resetAverages() {
    this.primaryAverage = -1;
    this.averageLoad = -1;
    this.minPrimaryImprovement = -1;
    this.minImprovement = -1;
  }

  /**
   * Calculate how much the variance in load will decrease for a given move.
   * 
   * @param sLoad
   *          the current load on the source member
   * @param sWeight
   *          the weight of the source member
   * @param tLoad
   *          the current load on the target member
   * @param tWeight
   *          the weight of the target member
   * @param bucketSize
   *          the size of the bucket we're considering moving
   * @param average
   *          the target weighted load for all members.
   * @return the change in variance that would occur by making this move.
   *         Essentially variance_before - variance_after, so a positive change
   *         is a means the variance is decreasing.
   */
  private double improvement(float sLoad, float sWeight,
      float tLoad, float tWeight, float bucketSize, float average) {
    
    double vSourceBefore = variance(sLoad, sWeight, average);
    double vSourceAfter = variance(sLoad - bucketSize, sWeight, average);
    double vTargetBefore = variance(tLoad, tWeight, average);
    double vTargetAfter  = variance(tLoad + bucketSize, tWeight, average);
    
    double improvement = vSourceBefore - vSourceAfter + vTargetBefore - vTargetAfter;
    return improvement / bucketSize;
  }
  
  private double variance(double load, double weight, double average) {
    double deviation = (load / weight - average) ;
    return deviation * deviation;
  }

  public Move findBestBucketMove() {
    Move bestMove= null;
    double bestImprovement = 0;
    for(Member source: this.members.values()) {
      for(Bucket bucket: source.getBuckets()) {
        for(Member target: this.members.values()) {
          if(bucket.getMembersHosting().contains(target)) {
            continue;
          }
          if(!target.willAcceptBucket(bucket, source, true).willAccept()) {
            continue;
          }
          double improvement = improvement(source.getTotalLoad(), source
              .getWeight(), target.getTotalLoad(), target.getWeight(), bucket.getLoad(),
              getAverageLoad());
          if (improvement > bestImprovement && improvement > getMinImprovement()) {
            Move move = new Move(source, target, bucket);
            if(!this.attemptedBucketMoves.contains(move)) {
              bestImprovement = improvement;
              bestMove = move;
            }
          }
        }
      }
    }
    return bestMove;
  }

  protected boolean moveBucket(Move bestMove) {
    Member bestSource = bestMove.getSource();
    Member bestTarget = bestMove.getTarget();
    BucketRollup bestBucket = (BucketRollup) bestMove.getBucket();

    Map<String, Long> colocatedRegionSizes = getColocatedRegionSizes(bestBucket);

    boolean successfulMove = this.operator.moveBucket(bestSource.getDistributedMember(), bestTarget
        .getDistributedMember(), bestBucket.getId(), colocatedRegionSizes);

    if(successfulMove) {
      bestBucket.addMember(bestTarget);
      if(bestSource.equals(bestBucket.getPrimary())) { 
        bestBucket.setPrimary(bestTarget, bestBucket.getPrimaryLoad());
      }
      bestBucket.removeMember(bestSource);
    }

    boolean entryAdded  = this.attemptedBucketMoves.add(bestMove);
    Assert
    .assertTrue(entryAdded,
        "PartitionedRegionLoadModel.moveBuckets - excluded set is not growing, so we probably would have an infinite loop here");
    
    return successfulMove;
  }

  /**
   * Return a snapshot of what the partitioned member details look like.
   * @return a set of partitioned member details.
   */
  public Set<PartitionMemberInfo> getPartitionedMemberDetails(String region) {
    TreeSet<PartitionMemberInfo> result = new TreeSet<PartitionMemberInfo>(); 
    for(MemberRollup member: this.members.values()) {
      Member colocatedMember = member.getColocatedMember(region);
      if(colocatedMember != null) {
        result.add(new PartitionMemberInfoImpl(colocatedMember
            .getDistributedMember(), colocatedMember
                .getConfiguredMaxMemory(), colocatedMember.getSize(),
                colocatedMember.getBucketCount(), colocatedMember
                    .getPrimaryCount()));
      }
    }
    return result;
  }
  
  /**
   * For testing only, calculate the total
   * variance of the members
   */
  public float getVarianceForTest() {
    float variance = 0;
    
    for(Member member: this.members.values()) {
      variance += variance(member.getTotalLoad(), member.getWeight(), getAverageLoad());
    }
    
    return variance;
  }
  
  /**
   * For testing only, calculate the total
   * variance of the members
   */
  public float getPrimaryVarianceForTest() {
    float variance = 0;
    
    for(Member member: this.members.values()) {
      variance += variance(member.getPrimaryLoad(), member.getWeight(), getPrimaryAverage());
    }
    
    return variance;
  }
  
  /**
   * Wait for the bucket operator to complete
   * any pending asynchronous operations.
   */
  public void waitForOperations() {
    operator.waitForOperations();
  }
  
  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    TreeSet<Bucket> allBucketIds = new TreeSet<Bucket>(new Comparator<Bucket>() {
      public int compare(Bucket o1, Bucket o2) {
        return o1.getId() - o2.getId();
      }
    });
    if(this.members.isEmpty()) {
      return "";
    }
    int longestMemberId = 0;
    for(Member member: this.members.values()) {
      allBucketIds.addAll(member.getBuckets());
      int memberIdLength = member.getDistributedMember().toString().length(); 
      if(longestMemberId < memberIdLength)  {
        longestMemberId = memberIdLength;
      }
    }
    result.append(String.format("%" + longestMemberId + "s primaries size(MB)  max(MB)", "MemberId"));
    for(Bucket bucket: allBucketIds) {
      result.append(String.format("%4s", bucket.getId()));
    }
    for(Member member: this.members.values()) {
      result.append(String.format("\n%" + longestMemberId
          + "s %9.0f %8.2f %8.2f", member
          .getDistributedMember(), member
          .getPrimaryLoad(), member.getSize() / (float)MEGABYTES, member
          .getConfiguredMaxMemory() / (float)MEGABYTES));
      for(Bucket bucket: allBucketIds) {
        char symbol;
        if(member.getPrimaryBuckets().contains(bucket)) {
          symbol = 'P';
        } else if(member.getBuckets().contains(bucket)){
          symbol = 'R';
        } else {
          symbol = 'X';
        }
        result.append("   ").append(symbol);
      }
    }
    
    result.append(String.format("\n%" + longestMemberId
        + "s                            ", "#offline", 0, 0, 0));
    for(Bucket bucket: allBucketIds) {
      result.append(String.format("%4s", bucket.getOfflineMembers().size()));
    }
   
    return result.toString();
  }
  
  /**
   * Represents the sum of all of the colocated regions on a given
   * member. Also, holds a map of all of the colocated regions
   * hosted on this member.
   */
  private class MemberRollup extends Member {
    private final Map<String, Member> colocatedMembers = new HashMap<String, Member>();
    private final boolean invalid = false;

    public MemberRollup(InternalDistributedMember memberId, boolean isCritical, boolean enforceLocalMaxMemory) {
      super(memberId, isCritical, enforceLocalMaxMemory);
    }
    
    /**
     * Indicates that this member doesn't have all of the colocated regions
     */
    public boolean isInvalid() {
      return invalid;
    }

    public boolean addColocatedMember(String region, Member member) {
      if(!getColocatedMembers().containsKey(region)) {
        this.getColocatedMembers().put(region, member);
        this.weight += member.weight;
        this.localMaxMemory += member.localMaxMemory;
        return true;
      }
      return false;
    }


    public Member getColocatedMember(String region) {
      return getColocatedMembers().get(region);
    }

    /**
     * Update the load on this member rollup with a change 
     * in size of one of the bucket rollups hosted by this member
     */
    public void updateLoad(float load, float primaryLoad, float bytes) {
      this.totalLoad += load;
      this.totalPrimaryLoad+= primaryLoad;
      this.totalBytes += bytes;
    }

    @Override
    public boolean addBucket(Bucket bucket) {
      if(super.addBucket(bucket)) {
        BucketRollup bucketRollup = (BucketRollup) bucket;
        for(Map.Entry<String, Member> entry: getColocatedMembers().entrySet()) {
          String region = entry.getKey();
          Member member = entry.getValue();
          Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
          if(colocatedBucket != null) {
            member.addBucket(colocatedBucket);
          }
        }
        return true;
      }
      return false;
    }
    
    @Override
    public boolean removeBucket(Bucket bucket) {
      if(super.removeBucket(bucket)) {
        BucketRollup bucketRollup = (BucketRollup) bucket;
        for(Map.Entry<String, Member> entry: getColocatedMembers().entrySet()) {
          String region = entry.getKey();
          Member member = entry.getValue();
          Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
          if(colocatedBucket != null) {
            member.removeBucket(colocatedBucket);
          }
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean addPrimary(Bucket bucket) {
      if(super.addPrimary(bucket)) {
        BucketRollup bucketRollup = (BucketRollup) bucket;
        for(Map.Entry<String, Member> entry: getColocatedMembers().entrySet()) {
          String region = entry.getKey();
          Member member = entry.getValue();
          Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
          if(colocatedBucket != null) {
            member.addPrimary(colocatedBucket);
          }
        }
        return true;
      }
      return false;
    }

    @Override
    public boolean removePrimary(Bucket bucket) {
      if(super.removePrimary(bucket)) {
        BucketRollup bucketRollup = (BucketRollup) bucket;
        for(Map.Entry<String, Member> entry: getColocatedMembers().entrySet()) {
          String region = entry.getKey();
          Member member = entry.getValue();
          Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
          if(colocatedBucket != null) {
            member.removePrimary(colocatedBucket);
          }
        }
        return true;
      }
      return false;
    }

    @Override
    public RefusalReason willAcceptBucket(Bucket bucket, Member source, boolean checkIPAddress) {
      RefusalReason reason = super.willAcceptBucket(bucket, source, checkIPAddress);
      if(reason.willAccept()) {
        BucketRollup bucketRollup = (BucketRollup) bucket;
        MemberRollup sourceRollup = (MemberRollup) source;
        for(Map.Entry<String, Member> entry: getColocatedMembers().entrySet()) {
          String region = entry.getKey();
          Member member = entry.getValue();
          Bucket colocatedBucket = bucketRollup.getColocatedBuckets().get(region);
          Member colocatedSource = sourceRollup == null ? null
              : sourceRollup.getColocatedMembers().get(region);
          if(colocatedBucket != null) {
            reason = member.willAcceptBucket(colocatedBucket, colocatedSource, checkIPAddress);
            if(!reason.willAccept()) {
              return reason;
            }
          }
        }
        return RefusalReason.NONE;
      }
      return reason;
    }

    Map<String, Member> getColocatedMembers() {
      return this.colocatedMembers;
    }
    
    
  }

  /**
   * Represents the sum of all of colocated buckets with
   * a given bucket id.
   * @author dsmith
   *
   */
  protected class BucketRollup extends Bucket {
    private final Map<String, Bucket> colocatedBuckets = new HashMap<String, Bucket>();

    public BucketRollup(int id) {
      super(id);
    }

    /**
     * @param region
     * @param b
     */
    public boolean addColocatedBucket(String region, Bucket b) {
      if(!this.getColocatedBuckets().containsKey(region)) {
        this.getColocatedBuckets().put(region, b);
        this.load += b.getLoad();
        this.primaryLoad += b.getPrimaryLoad();
        this.bytes += b.getBytes();
        this.offlineMembers.addAll(b.getOfflineMembers());
        
        //Update the load on the members hosting this bucket
        //to reflect the fact that the bucket is larger now.
        for(Member member: getMembersHosting()) {
          MemberRollup rollup = (MemberRollup) member;
          float primaryLoad = 0;
          if(this.getPrimary() == member) {
            primaryLoad = b.getPrimaryLoad();
          }
          rollup.updateLoad(b.getLoad(), primaryLoad, b.getBytes());
          
        }
        return true;
      }
      
      return false;
    }

    @Override
    public boolean addMember(Member targetMember) {
      if(super.addMember(targetMember)) {
        MemberRollup memberRollup = (MemberRollup) targetMember;
        for(Map.Entry<String, Bucket> entry: getColocatedBuckets().entrySet()) {
          String region = entry.getKey();
          Bucket bucket = entry.getValue();
          Member member = memberRollup.getColocatedMembers().get(region);
          if(member != null) {
            bucket.addMember(member);
          }
        }
        return true;
      }
      return false;
    }
    
    @Override
    public boolean removeMember(Member targetMember) {
      if(super.removeMember(targetMember)) {
        MemberRollup memberRollup = (MemberRollup) targetMember;
        for(Map.Entry<String, Bucket> entry: getColocatedBuckets().entrySet()) {
          String region = entry.getKey();
          Bucket bucket = entry.getValue();
          Member member = memberRollup.getColocatedMembers().get(region);
          if(member != null) {
            bucket.removeMember(member);
          }
        }
        return true;
      }
      return false;
    }

    @Override
    public void setPrimary(Member targetMember, float primaryLoad) {
      super.setPrimary(targetMember, primaryLoad);
      if(targetMember != null) {
        MemberRollup memberRollup = (MemberRollup) targetMember;
        for(Map.Entry<String, Bucket> entry: getColocatedBuckets().entrySet()) {
          String region = entry.getKey();
          Bucket bucket = entry.getValue();
          Member member = memberRollup.getColocatedMembers().get(region);
          if(member != null) {
            bucket.setPrimary(member, primaryLoad);
          }
      }
      }
    }

    Map<String, Bucket> getColocatedBuckets() {
      return this.colocatedBuckets;
    }
  }
  
  /**
   * Represents a single member of the distributed system.
   */
  protected class Member implements Comparable<Member> {
    private final InternalDistributedMember memberId;
    protected float weight;
    protected float totalLoad;
    protected float totalPrimaryLoad;
    protected long totalBytes;
    protected long localMaxMemory;
    private final Set<Bucket> buckets = new TreeSet<Bucket>();
    private final Set<Bucket> primaryBuckets = new TreeSet<Bucket>();
    private final boolean isCritical;
    private final boolean enforceLocalMaxMemory;
    
    public Member(InternalDistributedMember memberId, boolean isCritical, boolean enforceLocalMaxMemory) {
      this.memberId = memberId;
      this.isCritical = isCritical;
      this.enforceLocalMaxMemory = enforceLocalMaxMemory;
    }

    public Member(InternalDistributedMember memberId, float weight, long localMaxMemory, boolean isCritical, boolean enforceLocalMaxMemory) {
      this(memberId, isCritical, enforceLocalMaxMemory);
      this.weight = weight;
      this.localMaxMemory = localMaxMemory;
    }

    /**
     * @param bucket
     * @param sourceMember
     *          the member we will be moving this bucket off of
     * @param checkZone true if we should not put two copies
     * of a bucket on two nodes with the same IP address.
     */
    public RefusalReason willAcceptBucket(Bucket bucket, Member sourceMember, boolean checkZone) {
      //make sure this member is not already hosting this bucket
      if(getBuckets().contains(bucket)) {
        return RefusalReason.ALREADY_HOSTING;
      }
      // If node is not fully initialized yet, then skip this node (SQLFabric
      // DDL replay in progress).
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && cache.isUnInitializedMember(getMemberId())) {
        return RefusalReason.UNITIALIZED_MEMBER;
      }
      //Check the ip address
      if(checkZone) {
        //If the source member is equivalent to the target member, go
        //ahead and allow the bucket move (it's not making our redundancy worse).
        //TODO we could have some logic to prefer moving to different ip addresses
        //Probably that logic should be another stage after redundancy recovery, like
        //improveRedundancy.
        boolean sourceIsEquivalent = sourceMember != null
            && addressComparor.areSameZone(getMemberId(),
                sourceMember.getDistributedMember()); 
        if (sourceMember == null || !sourceIsEquivalent) {
          for(Member hostingMember : bucket.getMembersHosting()) {
            if ((!hostingMember.equals(sourceMember) || addressComparor.enforceUniqueZones())
                && addressComparor.areSameZone(getMemberId(),
                    hostingMember.getDistributedMember())) {
              if(logger.isDebugEnabled()) {
                logger.debug("Member {} would prefer not to host {} because it is already on another member with the same redundancy zone", this, bucket);
              }
              return RefusalReason.SAME_ZONE;
            }
          }
        }
      }

      //check the localMaxMemory
      if(this.enforceLocalMaxMemory && this.totalBytes + bucket.getBytes() > this.localMaxMemory) {
        if(logger.isDebugEnabled()) {
          logger.debug("Member {} won't host bucket {} because it doesn't have enough space", this, bucket);
        }
        return RefusalReason.LOCAL_MAX_MEMORY_FULL;
      }
      
      //check to see if the heap is critical
      if(isCritical) {
        if(logger.isDebugEnabled()) {
          logger.debug("Member {} won't host bucket {} because it's heap is critical",this, bucket);
        }
        return RefusalReason.CRITICAL_HEAP;
      }
      
      return RefusalReason.NONE;
    }

    public boolean addBucket(Bucket bucket) {
      if(getBuckets().add(bucket)) {
        bucket.addMember(this);
        this.totalBytes += bucket.getBytes();
        this.totalLoad += bucket.getLoad();
        return true;
      } 
      return false;
    }
    
    public boolean removeBucket(Bucket bucket) {
      if(getBuckets().remove(bucket)) {
        bucket.removeMember(this);
        this.totalBytes -= bucket.getBytes();
        this.totalLoad -= bucket.getLoad();
        return true;
      } 
      return false;
    }
    
    public boolean removePrimary(Bucket bucket) {
      if(getPrimaryBuckets().remove(bucket)) {
        this.totalPrimaryLoad -= bucket.getPrimaryLoad();
        return true;
      }
      return false;
    }

    public boolean addPrimary(Bucket bucket) {
      if(getPrimaryBuckets().add(bucket)) {
        this.totalPrimaryLoad += bucket.getPrimaryLoad();
        return true;
      }
      return false;
    }

    public int getBucketCount() {
      return getBuckets().size();
    }

    public long getConfiguredMaxMemory() {
      return this.localMaxMemory;
    }

    public InternalDistributedMember getDistributedMember() {
      return getMemberId();
    }

    public int getPrimaryCount() {
      int primaryCount = 0;
      for(Bucket bucket: getBuckets()) {
        if(this.equals(bucket.primary)) {
          primaryCount++;
        }
      }
      return primaryCount;
    }

    public long getSize() {
      return this.totalBytes;
    }
    
    public float getTotalLoad() {
      return this.totalLoad;
    }
    
    public float getWeight() {
      return this.weight;
    }
    
    @Override
    public String toString() {
      return "Member(id=" + getMemberId()+ ")";
    }

    public float getPrimaryLoad() {
      return this.totalPrimaryLoad;
    }

    protected Set<Bucket> getBuckets() {
      return this.buckets;
    }

    private InternalDistributedMember getMemberId() {
      return this.memberId;
    }

    private Set<Bucket> getPrimaryBuckets() {
      return this.primaryBuckets;
    }

    @Override
    public int hashCode() {
      return memberId.hashCode();
    }
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Member)) {
        return false;
      }
      Member o = (Member)other;
      return this.memberId.equals(o.memberId);
    }

    public int compareTo(Member other) {
      // memberId is InternalDistributedMember which implements Comparable
      return this.memberId.compareTo(other.memberId);
    }
  }

  /**
   * Represents a single bucket.
   */
  protected class Bucket implements Comparable<Bucket> {
    protected long bytes;
    private final int id;
    protected float load;
    protected float primaryLoad;
    private int redundancy = -1;
    private final Set<Member> membersHosting = new TreeSet<Member>();
    private Member primary;
    protected Set<PersistentMemberID> offlineMembers = new HashSet<PersistentMemberID>();
    
    public Bucket(int id) {
      this.id = id;
    }
    
    public Bucket(int id, float load, long bytes, Set<PersistentMemberID> offlineMembers) {
      this(id);
      this.load = load;
      this.bytes = bytes;
      this.offlineMembers = offlineMembers;
    }

    public void setPrimary(Member member, float primaryLoad) {
      if(this.primary == INVALID_MEMBER) {
        return;
      }
      if(this.primary != null) {
        this.primary.removePrimary(this);
      }
      this.primary = member;
      this.primaryLoad = primaryLoad;
      if(primary != INVALID_MEMBER && primary != null) {
        addMember(primary);
        member.addPrimary(this);
      }
    }

    /**
     * @param targetMember
     */
    public boolean addMember(Member targetMember) {
      if(this.getMembersHosting().add(targetMember)) {
        this.redundancy++;
        targetMember.addBucket(this);
        return true;
      }
      
      return false;
    }
    
    public boolean removeMember(Member targetMember) {
      if(this.getMembersHosting().remove(targetMember)) {
        if(targetMember == this.primary) {
          setPrimary(null, 0);
        }
        this.redundancy--;
        targetMember.removeBucket(this);
        return true;
      }
      return false;
    }
    
    public int getRedundancy() {
      return this.redundancy + offlineMembers.size();
    }
    
    public int getOnlineRedundancy() {
      return this.redundancy;
    }

    public float getLoad() {
      return this.load;
    }

    public int getId() {
      return this.id;
    }

    public long getBytes() {
      return this.bytes;
    }

    @Override
    public String toString() {
      return "Bucket(id=" + getId() + ",load=" + load +")";
    }

    public float getPrimaryLoad() {
      return this.primaryLoad;
    }
    
    public Set<Member> getMembersHosting() {
      return this.membersHosting;
    }
    
    public Member getPrimary() {
      return this.primary;
    }
    
    public Collection<? extends PersistentMemberID> getOfflineMembers() {
      return offlineMembers;
    }

    @Override
    public int hashCode() {
      return this.id;
    }
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Bucket)) {
        return false;
      }
      Bucket o = (Bucket)other;
      return this.id == o.id;
    }
    public int compareTo(Bucket other) {
      if (this.id < other.id) {
        return -1;
      } else if (this.id > other.id) {
        return 1;
      } else {
        return 0;
      }
    }
  }
  
  /**
   * Represents a move from one node to another. Used
   * to keep track of moves that we have already attempted
   * that have failed.
   * 
   * @author dsmith
   *
   */
  protected static class Move {
    private final Member source;
    private final Member target;
    private final Bucket bucket;
    
    public Move(Member source, Member target, Bucket bucket) {
      super();
      this.source = source;
      this.target = target;
      this.bucket = bucket;
    }
    
    
    /**
     * @return the source
     */
    public Member getSource() {
      return this.source;
    }


    /**
     * @return the target
     */
    public Member getTarget() {
      return this.target;
    }


    /**
     * @return the bucket
     */
    public Bucket getBucket() {
      return this.bucket;
    }


    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((this.bucket == null) ? 0 : this.bucket.hashCode());
      result = prime * result
          + ((this.source == null) ? 0 : this.source.hashCode());
      result = prime * result
          + ((this.target == null) ? 0 : this.target.hashCode());
      return result;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Move other = (Move) obj;
      if (this.bucket == null) {
        if (other.bucket != null)
          return false;
      } else if (!this.bucket.equals(other.bucket))
        return false;
      if (this.source == null) {
        if (other.source != null)
          return false;
      } else if (!this.source.equals(other.source))
        return false;
      if (this.target == null) {
        if (other.target != null)
          return false;
      } else if (!this.target.equals(other.target))
        return false;
      return true;
    }
    
    
    
  }
  
  public static interface AddressComparor {
    
    public boolean enforceUniqueZones();
    /**
     * Return true if the two addresses are equivalent
     */
   public boolean areSameZone(InternalDistributedMember member1, InternalDistributedMember member2); 
  }
  
  public static enum RefusalReason {
    NONE,
    ALREADY_HOSTING, UNITIALIZED_MEMBER,
    SAME_ZONE,
    LOCAL_MAX_MEMORY_FULL,
    CRITICAL_HEAP;
    
    public boolean willAccept() {
      return this == NONE;
    }
    
    public String formatMessage(Member source, Member target, Bucket bucket) {
      switch(this) {
      case NONE:
        return "No reason, the move should be allowed.";
      case ALREADY_HOSTING:
        return "Target member " + target.getMemberId()
            + " is already hosting bucket " + bucket.getId();
      case UNITIALIZED_MEMBER:
        return "Target member " + target.getMemberId()
            + " is not fully initialized";
      case SAME_ZONE:
        return "Target member "
            + target.getMemberId()
            + " is in the same redundancy zone as other members hosting bucket "
            + bucket.getId() + ": " + bucket.getMembersHosting();
      case LOCAL_MAX_MEMORY_FULL:
        return "Target member " + target.getMemberId()
            + " does not have space within it's local max memory for bucket "
            + bucket.getId() + ". Bucket Size " + bucket.getBytes()
            + " local max memory: " + target.localMaxMemory + " remaining: "
            + target.totalBytes;
      case CRITICAL_HEAP:
        return "Target member "
            + target.getMemberId()
            + " has reached its critical heap percentage, and cannot accept more data"; 
      default:
        return this.toString();
      }
    }
  }
}
