/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.partitioned.rebalance;

import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Bucket;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Member;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.Move;
import com.gemstone.gemfire.internal.cache.partitioned.rebalance.PartitionedRegionLoadModel.RefusalReason;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class ExplicitMoveDirector extends RebalanceDirectorAdapter {
  
  private PartitionedRegionLoadModel model;
  private final int bucketId;
  private final InternalDistributedMember source;
  private final InternalDistributedMember target;
  private final Object key;
  private InternalDistributedSystem ds;

  
  public ExplicitMoveDirector(Object key, int bucketId, DistributedMember source,
      DistributedMember target, DistributedSystem distributedSystem) {
    this.key = key;
    this.bucketId = bucketId;
    this.source = (InternalDistributedMember) source;
    this.target = (InternalDistributedMember) target;
    this.ds = (InternalDistributedSystem) distributedSystem;
  }

  @Override
  public void initialize(PartitionedRegionLoadModel model) {
    this.model = model;
  }

  @Override
  public void membershipChanged(PartitionedRegionLoadModel model) {
    initialize(model);
  }

  @Override
  public boolean nextStep() {
    Bucket bucket = model.getBuckets()[bucketId];
    Member sourceMember = model.getMember(source);
    Member targetMember = model.getMember(target);
    if(sourceMember == null) {
      throw new IllegalStateException(LocalizedStrings.PERCENTAGE_MOVE_DIRECTORY_SOURCE_NOT_DATA_STORE.toLocalizedString(model.getName(), source));
    }
    if(targetMember == null) {
      throw new IllegalStateException(LocalizedStrings.PERCENTAGE_MOVE_DIRECTORY_TARGET_NOT_DATA_STORE.toLocalizedString(model.getName(), target));
    }
    
    if(bucket == null) {
      throw new IllegalStateException("The bucket for key " + key + ", bucket " + bucketId + ", region " + model.getName() + " does not exist");
    }
    
    if(!bucket.getMembersHosting().contains(sourceMember)) {
      throw new IllegalStateException("The bucket for key " + key + ", bucket " + bucketId + ", region " + model.getName() + " is not hosted by " + source + ". Members hosting: " + bucket.getMembersHosting());
    }
    
    RefusalReason reason = targetMember.willAcceptBucket(bucket, sourceMember, model.enforceUniqueZones());
    if(reason.willAccept()) {
      if(!model.moveBucket(new Move(sourceMember, targetMember, bucket))) {
        //Double check to see if the source or destination have left the DS
        Set allMembers = ds.getDistributionManager().getDistributionManagerIdsIncludingAdmin();
        if(!allMembers.contains(sourceMember)) {
          throw new IllegalStateException(LocalizedStrings.PERCENTAGE_MOVE_DIRECTORY_SOURCE_NOT_DATA_STORE.toLocalizedString(model.getName(), source));
        }
        if(!allMembers.contains(targetMember)) {
          throw new IllegalStateException(LocalizedStrings.PERCENTAGE_MOVE_DIRECTORY_TARGET_NOT_DATA_STORE.toLocalizedString(model.getName(), target));
        }
        throw new IllegalStateException("Unable to move bucket " + bucket + " from " + sourceMember + " to " + targetMember);
      } 
    } else {
      throw new IllegalStateException("Unable to move bucket for " + model.getName() + ". " + reason.formatMessage(sourceMember, targetMember, bucket));
    }
    
    return false;
  }
}
